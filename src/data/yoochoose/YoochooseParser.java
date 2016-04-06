package data.yoochoose;

import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import au.com.bytecode.opencsv.CSVReader;
/**
 * See http://2015.recsyschallenge.com/
 * 
 * Dataset in .7z format available here:
 * http://s3-eu-west-1.amazonaws.com/yc-rdata/yoochoose-dataFull.7z
 * 
 * And the solution is here: http://s3-eu-west-1.amazonaws.com/yc-rdata/yoochoose-solution.dat
 * 
 * We generate one file type currently - Vowpal Wabbit:
 * 
 * VW format: https://github.com/JohnLangford/vowpal_wabbit/wiki/Input-format
 * 
 * @author hsheil
 */
public class YoochooseParser {

  private static final Logger LOG = LoggerFactory.getLogger(YoochooseParser.class);

  private static final int NUM_EVENTS = 400;

  private static final long LOG_INTERVAL = 5_000L;

  private Set<Integer> uniques;
  
  private Map<Integer, Integer> similar;

  Map<Integer, Integer> itemsPurchased;
  
  Map<Integer, Integer> categoriesBrowsed;

  public YoochooseParser() {
    uniques = new HashSet<>();
    itemsPurchased = new HashMap<>();
    categoriesBrowsed = new HashMap<>();
    //We always want the highest count first
    similar = new TreeMap<>(Collections.reverseOrder());
  }

  public void buildSaveVwFile(Map<Integer, List<Event>> visitors, String inFName, boolean isTrain) {
    LOG.info("Creating VW file from data loaded");
    long startTime = System.currentTimeMillis();
    long currTime = startTime;
    int i = 0;
    int j = 0;

    try (PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(inFName)));) {
      for (Map.Entry<Integer, List<Event>> entry : visitors.entrySet()) {
        List<Event> events = entry.getValue();
        StringBuilder sb = new StringBuilder();
        Event lastE = events.get(events.size() - 1);
        
        //Label [Importance] [Base] ['Tag]
        if (isTrain){
          if (lastE instanceof Purchase) {
            //Bump the weight of purchaser example - improves (TP - FP) by
            //~16.67% (28546 vs 24,466) on the full training set
            sb.append("1 1.4");
          } else {
            sb.append("0 1.0");
          }
          sb.append(" '"+entry.getKey()+"|");
        } else {
          sb.append("'"+entry.getKey()+"|");
        }

        sb.append("AggregateFeatures numClicks:" + events.size() + " lifespan:"
            + calculateDuration(events.get(0), events.get(events.size() - 1)));

        LocalDateTime ldt1 = events.get(0).getDate();
        LocalDateTime ldt2 = events.get(events.size() - 1).getDate();

        // Now add in date / time features that span the session
//        sb.append(" sMonth:" + ldt1.getMonthValue() + " sDay:" + ldt1.getDayOfMonth() + " sWeekDay:" + ldt1.getDayOfWeek().getValue()+" sHour:"
//            + ldt1.getHour() + " sMin:" + ldt1.getMinute() + " sSec:" + ldt1.getSecond());
//
//        sb.append(" eMonth:" + ldt2.getMonthValue() + " eDay:" + ldt2.getDayOfMonth() + " eWeekDay:" + ldt2.getDayOfWeek().getValue()+" eHour:"
//            + ldt2.getHour() + " eMin:" + ldt2.getMinute() + " eSec:" + ldt2.getSecond());

        // Now add in # unique items and categories
        sb.append(" numItems:" + getItems(events) + " numCategories:" + getCategories(events));

        // Rough approximation for popular, purchased items
        sb.append(" viewedPopularItems:" + (didViewPopularItems(events) ? 1.0 : 0.0));

        // Rough approximation for popular, purchased categories
        sb.append(" viewedPopularCats:" + (didViewPopularCats(events) ? 1.0 : 0.0));
        
        // Rough approximation for content similarity by category
        sb.append(" catSimilarity:" + calcCatSimilarity(events));


        // Now add in the events themselves
        int eLimit = NUM_EVENTS;
        if (events.size() < NUM_EVENTS) {
          eLimit = events.size();
        }
        for (int k = 0; k < eLimit; k++) {
          Event e = events.get(k);
          Event nextE = null;
          if (k < events.size() - 1) {
            nextE = events.get(k + 1);
          }

          long duration = 100L;
          if (nextE != null) {
            duration = calculateDuration(e, nextE);
          }

          LocalDateTime ldt = e.getDate();
          //sb.append(" |Event" + k + " mth:" + ldt.getMonthValue() + " day:" + ldt.getDayOfMonth()+ " hour:" +ldt.getHour());
          sb.append(" |Event" + k);
          sb.append(" itemId:" + e.getItemId() + " dwellTime:" + duration);
          if (e instanceof Click) {
            Click c = (Click) e;
            sb.append(
                " catId:" + c.getCategoryId() + " special:" + (c.isSpecial() ? "1.0" : "0.0"));
          }
        }
        sb.append("\n");
        out.write(sb.toString());
        i++;
        currTime = System.currentTimeMillis();
        if ((currTime - startTime) > LOG_INTERVAL) {
          LOG.info("{} sessions processed (rate: {}/sec)", i,
              (float) ((i - j) / LOG_INTERVAL) * 1_000);
          startTime = currTime;
          j = i;
        }

      }

    } catch (IOException e) {
      LOG.error("Error writing vw file", e);
    }

  }

  private int calcCatSimilarity(List<Event> events) {
    
    int rVal=0;
    similar.clear();
    for (Event e : events){
      if (e instanceof Click){
        Click c = (Click)e;
        int quantity = 1;
        if (similar.containsKey(c.getCategoryId())){
          quantity = similar.get(c.getCategoryId())+1;
        }
        similar.put(c.getCategoryId(), quantity);
      }
      
      Map<Integer, Integer> sorted = sortByValue(Collections.reverseOrder(), 1_000, similar);
      for (Map.Entry<Integer, Integer> entry : sorted.entrySet()){
        //This should be the highest frequency of category
        return entry.getValue();
      }
    }
    return rVal;
  }

  private boolean didViewPopularItems(List<Event> events) {
    for (Event e : events){
      //This code works iff analyse() called (side-effect)..
      //TODO refactor this
      if (itemsPurchased.containsValue(e.getItemId())){
        return true;
      }
    }
    return false;
  }
  
  private boolean didViewPopularCats(List<Event> events) {
    for (Event e : events){
      //This code works iff analyse() called (side-effect)..
      //TODO refactor this
      if (e instanceof Click){
        Click c = (Click)e;
        if (categoriesBrowsed.containsValue(c.getCategoryId())){
          return true;
        }
      }
    }
    return false;
  }
  

  private int getCategories(List<Event> events) {
    for (Event e : events) {
      if (e instanceof Click) {
        Click c = (Click) e;
        int x = c.getCategoryId();
        uniques.add(x);
      }
    }
    int rVal = uniques.size();
    uniques.clear();
    return rVal;
  }

  private int getItems(List<Event> events) {
    for (Event e : events) {
      int x = e.getItemId();
      uniques.add(x);
    }
    int rVal = uniques.size();
    uniques.clear();
    return rVal;
  }

  private long calculateDuration(Event e1, Event e2) {

    if (e1 == null || e2 == null) {
      return 0l;
    }
    LocalDateTime ldt1 = e1.getDate();
    LocalDateTime ldt2 = e2.getDate();
    if (ldt1 == null || ldt2 == null) {
      return 0L;
    }

    return ldt1.until(ldt2, ChronoUnit.SECONDS);
  }

  public void analyse(Map<Integer, List<Event>> visitors) {
    // I know 262 is the max from inspecting the data, hence why we use 270 here..
    Map<Integer, Integer> eventBuckets = new HashMap<>(270);


    long allMins = 0;
    long average = 0;
    long purchaserMins = 0;
    long singleClick = 0;
    long singlePurchase = 0;
    long unknownEventType = 0;
    long avgEvents = 0;
    int maxEvents = 0;
    for (Map.Entry<Integer, List<Event>> e : visitors.entrySet()) {
      List<Event> events = e.getValue();
      
      //Just looking for rogue / not-so-useful data
      if (e.getValue().size() <= 1) {
        Event event = e.getValue().get(0);
        if (event instanceof Click) {
          singleClick++;
        } else if (event instanceof Purchase) {
          singlePurchase++;
        }
        continue;
      }
      long mins = calculateDuration(events.get(0), events.get(events.size() - 1));
      int numEvents = events.size();
      average += mins;
      avgEvents += numEvents;
      if (numEvents > maxEvents) {
        maxEvents = numEvents;
      }
      int count = 1;
      if (eventBuckets.containsKey(numEvents)) {
        count += eventBuckets.get(numEvents);
      }

      if (mins > allMins) {
        allMins = mins;
      }
      // Now look at Purchase data
      Event e2 = events.get(events.size() - 1);
      if (e2 instanceof Purchase) {
        int itemCount = 1;
        if (itemsPurchased.containsKey(e2.getItemId())) {
          itemCount = itemsPurchased.get(e2.getItemId()) + 1;
        }
        itemsPurchased.put(e2.getItemId(), itemCount);
        
        // We only count events for purchasers
        eventBuckets.put(numEvents, count);
        if (mins > purchaserMins) {
          purchaserMins = mins;
        }
        //Only calculate popular cats for purchasers
        for (Event event : events){
          if (event instanceof Click){
            Click c = (Click) event;
            int quantity = 1;
            if (categoriesBrowsed.containsKey(c.getCategoryId())){
              quantity = categoriesBrowsed.get(c.getCategoryId())+1;
            }
            categoriesBrowsed.put(c.getCategoryId(), quantity);
          }
        }

      }
    }
    //Get rid of 0 as it is not a real category - it represents data not present
    categoriesBrowsed.remove(0);
    LOG.info(
        "All max: {} secs, purchasers max: {} secs, avg: {} secs, single click {}, single purchase {}, unknown {}, max events {}, avg events {}",
        allMins, purchaserMins, (float) average / visitors.size(), singleClick, singlePurchase,
        unknownEventType, maxEvents, (float) avgEvents / visitors.size());
    // LOG.info("Top 70 Event buckets are: \n{}", output(eventBuckets, numPurchasers, 70));
    //We use this map later to calculate a popularity feature in the output file
    itemsPurchased = sortByValue(Collections.reverseOrder(), 400, itemsPurchased);
    categoriesBrowsed = sortByValue(Collections.reverseOrder(), 100, categoriesBrowsed);
    LOG.info("Top 400 items purchased: \n{}", output(itemsPurchased));
    LOG.info("Top 100 cats browsed: \n{}", output(categoriesBrowsed));
  }
  
  private Map<Integer, Integer> sortByValue(Comparator<Integer> inC, int inLimit, Map<Integer, Integer> inSrc){
    // Sort the map..
    Map<Integer, Integer> sorted = new TreeMap<>(inC);
    for (Map.Entry<Integer, Integer> e : inSrc.entrySet()) {
      // We deliberately swap the ks and vs here
      // as we now want to sort by frequency and not unique ID anymore
      sorted.put(e.getValue(), e.getKey());
    }
    //Now we remove anything after the limit
    Map<Integer, Integer> rVal = new TreeMap<>(inC);
    for (Map.Entry<Integer, Integer> e : sorted.entrySet()) {
      if (inLimit <= 0){
        break;
      }
      rVal.put(e.getKey(), e.getValue());
      inLimit--;
    }
    
    return rVal;
  }

  private Object output(Map<Integer, Integer> inBuckets) {
    StringBuilder sb = new StringBuilder();

    for (Map.Entry<Integer, Integer> e : inBuckets.entrySet()) {
      sb.append(
          e.getKey() + ":" + e.getValue() + "\n");
    }
    return sb.toString();
  }

  /**
   * Handles the clicks, buys, test and solution *.dat files in the yoochoose 7z file.
   * 
   * @param visitors
   * @param inFname
   * @param inT
   */
  public void buildMap(Map<Integer, List<Event>> visitors, String inFname, Event.Type inT,
      char inSeparatorChar) {

    String format = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";
    DateTimeFormatter dtf = DateTimeFormatter.ofPattern(format);
    long startTime1 = System.currentTimeMillis();
    long startTime2 = System.currentTimeMillis();
    long currTime = 0L;
    int i = 0;
    int j = 0;
    LOG.info("Loading {}", inFname);
    String[] nextLine = null;
    try (CSVReader reader = new CSVReader(new FileReader(inFname), inSeparatorChar)) {
      while ((nextLine = reader.readNext()) != null) {

        // Handle dodgy blank lines - seem to only be in solution.dat
        if ("".equals(nextLine[0])) {
          continue;
        }

        int vId = Integer.parseInt(nextLine[0]);
        LocalDateTime dt = null;

        try {
          dt = LocalDateTime.parse(nextLine[1], dtf);
        } catch (DateTimeParseException e) {
          // This is an expected occurrence for the solution.dat file
        }
        switch (inT) {
          case CLICK:
            Click c = new Click();
            c.setDate(dt);
            c.setItemId(Integer.parseInt(nextLine[2]));
            if ("S".equals(nextLine[3])) {
              c.setSpecial(true);
            } else {
              c.setCategoryId(Integer.parseInt(nextLine[3]));
            }
            add(visitors, c, vId);
            break;
          case PURCHASE:
            Purchase p = new Purchase();
            p.setDate(dt);
            try {
              p.setItemId(Integer.parseInt(nextLine[2]));
              p.setPrice(Integer.parseInt(nextLine[3]));
              p.setQuantity(Integer.parseInt(nextLine[3]));
            } catch (ArrayIndexOutOfBoundsException e) {
              // Expected for solution.dat - I hate myself for writing this code
            }
            add(visitors, p, vId);
            break;
          default:
            LOG.error("Unsupported event type encountered");
            break;
        }

        currTime = System.currentTimeMillis();
        i++;
        if ((currTime - startTime2) > LOG_INTERVAL) {
          LOG.info("{} events processed (rate: {}/sec)", i,
              (float) ((i - j) / LOG_INTERVAL) * 1_000);
          startTime2 = currTime;
          j = i;
        }
      }

      LOG.info("{} data lines -> {} visitors, processed in {} secs", i, visitors.keySet().size(),
          (currTime - startTime1) / 1000);
    } catch (Exception ie) {
      LOG.error("Error at line {} in file", i, ie);
      LOG.error("Complete line is: '{},{},{},{}'", nextLine[0], nextLine[1], nextLine[2],
          nextLine[3]);
    }


  }

  private void add(Map<Integer, List<Event>> visitors, Event inE, int vId) {
    if (visitors.containsKey(vId)) {
      visitors.get(vId).add(inE);
    } else {
      List<Event> nL = new ArrayList<>();
      nL.add(inE);
      visitors.put(vId, nL);
    }
  }

}
