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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
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
 * We generate two file types currently - Vowpal Wabbit and LIBSVM (used by XGBoost):
 * 
 * VW format: https://github.com/JohnLangford/vowpal_wabbit/wiki/Input-format
 * 
 * LIBSVM format: https://github.com/dmlc/xgboost/blob/master/doc/input_format.md
 * 
 * @author hsheil
 */
public class YoochooseParser {

  private static final Logger LOG = LoggerFactory.getLogger(YoochooseParser.class);

  private static final int NUM_EVENTS = 400;

  private static final long LOG_INTERVAL = 5_000L;

  private static final String BUYER_LABEL = "1";

  private static final String CLICKER_LABEL = "0";

  private static final String VW_DELIMITER = "|";

  private static final String FEAT_SEP = " ";

  private static final String FEAT_VAL_SEP = ":";

  private Set<Integer> uniques;

  private Map<Integer, Integer> similar;

  private Map<String, Integer> labelMappings;

  private int labelCounter;

  private Map<Integer, Integer> itemsPurchased;

  private Map<Integer, Integer> categoriesBrowsed;
  
  private Map<Integer, Integer> mostPopularItems;
  
  private Map<Integer, Integer> mostPopularCategories;

  private Map<Integer, List<Event>> clickers;
  private Map<Integer, List<Event>> buyers;

  private final Mode mode;

  private final Format format;

  private final boolean balanced;


  public YoochooseParser(int inClickers, int inBuyers, Format inF, Mode inM, boolean inBalanced) {
    uniques = new HashSet<>();
    itemsPurchased = new HashMap<>();
    categoriesBrowsed = new HashMap<>();
    mostPopularItems = new HashMap<>();
    mostPopularCategories = new HashMap<>();
    // We always want the highest count first
    similar = new TreeMap<>(Collections.reverseOrder());
    clickers = new HashMap<>(inClickers);
    buyers = new HashMap<>(inBuyers);
    labelMappings = new HashMap<>();
    mode = inM;
    format = inF;
    balanced = inBalanced;
  }

  public void output(String inFName) {
    LOG.info("Creating {} file from data loaded", format);
    long startTime = System.currentTimeMillis();
    long currTime = startTime;
    int i = 0;
    int j = 0;

    try (PrintWriter mainFile = new PrintWriter(new BufferedWriter(new FileWriter(inFName)));
        PrintWriter labelsFile =
            new PrintWriter(new BufferedWriter(new FileWriter(inFName + ".label")))) {
      // We need a separate iterator for buyers
      Iterator<Entry<Integer, List<Event>>> buyerEntries = buyers.entrySet().iterator();
      for (Map.Entry<Integer, List<Event>> clickerEntry : clickers.entrySet()) {
        writeSession(clickerEntry, mainFile, format, mode);
        writeLabel(clickerEntry.getKey(), labelsFile);
        i++;

        // Only iterate over buyers collxn in TRAIN mode - in TEST mode it will be empty
        if (Mode.TRAIN.equals(mode)) {
          if (!buyerEntries.hasNext() && balanced) {
            // In balanced mode, refresh the buyer iterator if we've exhausted it
            // as there are always far more (95:5) clickers than buyers
            buyerEntries = buyers.entrySet().iterator();
          }
          if (buyerEntries.hasNext()) {
            Map.Entry<Integer, List<Event>> buyerEntry = buyerEntries.next();
            writeSession(buyerEntry, mainFile, format, mode);
            writeLabel(buyerEntry.getKey(), labelsFile);
            i++;
          }
        }
        currTime = System.currentTimeMillis();
        if ((currTime - startTime) > LOG_INTERVAL) {
          LOG.info("{} sessions processed (rate: {}/sec)", i,
              (float) ((i - j) / LOG_INTERVAL) * 1_000);
          startTime = currTime;
          j = i;
        }
      }
    } catch (IOException e) {
      LOG.error("Error writing file", e);
    }
  }

  private void writeLabel(Integer inSessionId, PrintWriter out2) {
    StringBuilder sb = new StringBuilder();
    sb.append(inSessionId + "\n");
    out2.write(sb.toString());
  }

  private void writeSession(Entry<Integer, List<Event>> entry, PrintWriter out, Format inF,
      Mode inM) {
    List<Event> events = entry.getValue();
    StringBuilder sb = new StringBuilder();
    Event lastE = events.get(events.size() - 1);

    Integer visitorId = entry.getKey();

    boolean buyer = false;
    if (lastE instanceof Purchase) {
      buyer = true;
    }
    sb.append(buildStart(buyer, inM, inF, visitorId));

    // Output session-level features
    sb.append(buildSessionFeatures(inF, events));

    // Now transform and output the events themselves
    sb.append(buildEvents(inF, events));

    sb.append("\n");
    out.write(sb.toString());

    // Write out the session ID as a comment for LIBSVM
    StringBuilder sb2 = new StringBuilder();
    sb2.append(visitorId);

  }

  private StringBuilder buildEvents(Format inF, List<Event> events) {
    StringBuilder sb = new StringBuilder();
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
      String prefix = "";
      if (Format.VW.equals(inF)) {
        sb.append("|Event" + k + FEAT_SEP);
      } else {
        prefix += "Event" + k + FEAT_SEP;
      }
      append(sb, prefix + "mth", ldt.getMonthValue());
      append(sb, prefix + "day", ldt.getDayOfMonth());
      append(sb, prefix + "hour", ldt.getHour());
      append(sb, prefix + "minute", ldt.getMinute());
      append(sb, prefix + "second", ldt.getSecond());
      append(sb, prefix + e.getItemId() + "-itemId", 1);
      append(sb, prefix + e.getItemId() + "item-was-purchased", wasPurchased(e.getItemId()) ? 1 : 0);
      append(sb, prefix + "dwellTime", duration);
      if (e instanceof Click) {
        Click c = (Click) e;
        append(sb, prefix + c.getCategoryId() + "-catId", 1);
        append(sb, prefix + "special", c.isSpecial() ? 1 : 0);
        append(sb, prefix + "cat-category", simplifyCategory(c));
      }
    }
    return sb;
  }

  private boolean wasPurchased(int itemId) {
    return itemsPurchased.containsKey(itemId);
  }

  /**
   * Simplifies event categories into 4 simple buckets - brand, 1 - 12, special and not present
   * 
   * @param inC
   * @return
   */
  private int simplifyCategory(Click inC) {
    int categoryId = inC.getCategoryId();
    if (categoryId == 0) {
      // Data not present
      return 1;
    } else if (categoryId < 12 && categoryId > 0) {
      // One of 12
      return 2;
    } else if (inC.isSpecial()) {
      // Special
      return 3;
    } else {
      // Must be brand
      return 4;
    }
  }

  private StringBuilder buildSessionFeatures(Format inF, List<Event> events) {
    StringBuilder sb = new StringBuilder();
    sb.append(mapLabel("AggregateFeatures numClicks") + FEAT_VAL_SEP + events.size() + FEAT_SEP
        + mapLabel("lifespan") + FEAT_VAL_SEP
        + calculateDuration(events.get(0), events.get(events.size() - 1)));

    LocalDateTime ldt1 = events.get(0).getDate();
    LocalDateTime ldt2 = events.get(events.size() - 1).getDate();

    // Now add in date / time features that span the session
    append(sb, "sMonth", ldt1.getMonthValue());
    append(sb, "sDay", ldt1.getDayOfMonth());
    append(sb, "sWeekDay", ldt1.getDayOfWeek().getValue());
    append(sb, "sHour", ldt1.getHour());
    append(sb, "sMin", ldt1.getMinute());
    append(sb, "sSec", ldt1.getSecond());

    append(sb, "eMonth", ldt2.getMonthValue());
    append(sb, "eDay", ldt2.getDayOfMonth());
    append(sb, "eWeekDay", ldt2.getDayOfWeek().getValue());
    append(sb, "eHour", ldt2.getHour());
    append(sb, "eMin", ldt2.getMinute());
    append(sb, "eSec", ldt2.getSecond());

    // Now add in # unique items and categories
    append(sb, "numItems", getUniqueItems(events));
    append(sb, "numCategories", getUniqueCategories(events));


    // Rough approximation for popular, purchased items
    append(sb, "viewedPopularItems", didViewPopularItems(events) ? 1.0 : 0.0);

    // Rough approximation for popular, purchased categories
    append(sb, "viewedPopularCats", didViewPopularCats(events) ? 1.0 : 0.0);

    // Rough approximation for content similarity by category
    append(sb, "catSimilarity", calcCatSimilarity(events));

    return sb;
  }

  private void append(StringBuilder sb, String inL, Object inValue) {
    sb.append(FEAT_SEP + mapLabel(inL) + FEAT_VAL_SEP + inValue.toString());
  }

  /**
   * Maps all string inputs into a number space for LIBSVM format. A mapping is used consistently
   * within a run, but is not guaranteed to be the same across multiple runs.
   * 
   * @param inL
   * @return
   */
  private String mapLabel(String inL) {
    switch (format) {
      case VW:
        return inL;
      case LIBSVM:
        if (!labelMappings.containsKey(inL)) {
          labelMappings.put(inL, labelCounter++);
        }
        return labelMappings.get(inL).toString();
      default:
        return "";
    }
  }

  /**
   * Create the initial part of each line in the training / test file - [class label], [label
   * importance weighting], [a tag to know which session this line relates to]. All field here are
   * optional as (a) LIBSVM format does not support tags as VW does (xgboost errors and fails to
   * parse), and for test files for both we don't know the labels for sessions.
   * 
   * @param isBuyer
   * @param inM
   * @param inF
   * @param inVisitorId
   * @return
   */
  private String buildStart(boolean isBuyer, Mode inM, Format inF, Integer inVisitorId) {
    String label = isBuyer ? BUYER_LABEL : CLICKER_LABEL;
    switch (inF) {
      case VW:
        if (Mode.TRAIN.equals(inM)) {
          // Label [Importance] [Base] ['Tag]
          return label + " 1.0 '" + inVisitorId + VW_DELIMITER;
        } else {
          return "'" + inVisitorId + VW_DELIMITER;
        }
      case LIBSVM:
        return label + FEAT_SEP;
      default:
        return "";
    }
  }

  private int calcCatSimilarity(List<Event> events) {

    int rVal = 0;
    similar.clear();
    for (Event e : events) {
      if (e instanceof Click) {
        Click c = (Click) e;
        int quantity = 1;
        if (similar.containsKey(c.getCategoryId())) {
          quantity = similar.get(c.getCategoryId()) + 1;
        }
        similar.put(c.getCategoryId(), quantity);
      }

      Map<Integer, Integer> sorted = sortByValue(Collections.reverseOrder(), 1_000, similar);
      for (Map.Entry<Integer, Integer> entry : sorted.entrySet()) {
        // This should be the highest frequency of category
        return entry.getValue();
      }
    }
    return rVal;
  }

  private boolean didViewPopularItems(List<Event> events) {
    for (Event e : events) {
      if (mostPopularItems.containsValue(e.getItemId())) {
        return true;
      }
    }
    return false;
  }

  private boolean didViewPopularCats(List<Event> events) {
    for (Event e : events) {
      if (e instanceof Click) {
        Click c = (Click) e;
        if (mostPopularItems.containsValue(c.getCategoryId())) {
          return true;
        }
      }
    }
    return false;
  }

  private int getUniqueCategories(List<Event> events) {
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

  private int getUniqueItems(List<Event> events) {
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

  public void analyse() {
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
    // TODO support both clickers and buyers..
    for (Map.Entry<Integer, List<Event>> e : clickers.entrySet()) {
      List<Event> events = e.getValue();

      // Just looking for rogue / not-so-useful data
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
        // Only calculate popular cats for purchasers
        for (Event event : events) {
          if (event instanceof Click) {
            Click c = (Click) event;
            int quantity = 1;
            if (categoriesBrowsed.containsKey(c.getCategoryId())) {
              quantity = categoriesBrowsed.get(c.getCategoryId()) + 1;
            }
            categoriesBrowsed.put(c.getCategoryId(), quantity);
          }
        }

      }
    }
    // Get rid of 0 as it is not a real category - it represents data not present
    categoriesBrowsed.remove(0);
    LOG.info(
        "All max: {} secs, purchasers max: {} secs, avg: {} secs, single click {}, single purchase {}, unknown {}, max events {}, avg events {}",
        allMins, purchaserMins, (float) average / clickers.size(), singleClick, singlePurchase,
        unknownEventType, maxEvents, (float) avgEvents / clickers.size());
    // LOG.info("Top 70 Event buckets are: \n{}", output(eventBuckets, numPurchasers, 70));
    // We use this map later to calculate a popularity feature in the output file
    mostPopularItems = sortByValue(Collections.reverseOrder(), 400, itemsPurchased);
    mostPopularCategories = sortByValue(Collections.reverseOrder(), 100, categoriesBrowsed);
    LOG.info("Top 400 items purchased: \n{}", mapToString(mostPopularItems));
    LOG.info("Top 100 cats browsed: \n{}", mapToString(mostPopularCategories));
    LOG.info("Unique items purchased: {}", itemsPurchased.size());
    LOG.info("Unique categories: \n{}", categoriesBrowsed.size());

  }

  private Map<Integer, Integer> sortByValue(Comparator<Integer> inC, int inLimit,
      Map<Integer, Integer> inSrc) {
    // Sort the map..
    Map<Integer, Integer> sorted = new TreeMap<>(inC);
    for (Map.Entry<Integer, Integer> e : inSrc.entrySet()) {
      // We deliberately swap the ks and vs here
      // as we now want to sort by frequency and not unique ID anymore
      sorted.put(e.getValue(), e.getKey());
    }
    // Now we remove anything after the limit
    Map<Integer, Integer> rVal = new TreeMap<>(inC);
    for (Map.Entry<Integer, Integer> e : sorted.entrySet()) {
      if (inLimit <= 0) {
        break;
      }
      rVal.put(e.getKey(), e.getValue());
      inLimit--;
    }

    return rVal;
  }

  private Object mapToString(Map<Integer, Integer> inBuckets) {
    StringBuilder sb = new StringBuilder();

    for (Map.Entry<Integer, Integer> e : inBuckets.entrySet()) {
      sb.append(e.getKey() + ":" + e.getValue() + "\n");
    }
    return sb.toString();
  }

  /**
   * Handles the clicks, buys, test and solution *.dat files in the yoochoose 7z file.
   * 
   * @param inFname
   * @param inT
   */
  public void buildMap(String inFname, Event.Type inT, char inSeparatorChar) {

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

        // Ignore blank lines (which only occur in solution.dat)
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
            add(c, vId);
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
            move(p, vId);
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

      LOG.info("{} data lines -> {} visitors, processed in {} secs", i,
          (clickers.keySet().size() + buyers.keySet().size()), (currTime - startTime1) / 1000);
    } catch (Exception ie) {
      LOG.error("Error at line {} in file", i, ie);
      LOG.error("Complete line is: '{},{},{},{}'", nextLine[0], nextLine[1], nextLine[2],
          nextLine[3]);
    }
  }

  private void add(Event inE, int vId) {
    if (clickers.containsKey(vId)) {
      clickers.get(vId).add(inE);
    } else {
      List<Event> nL = new ArrayList<>();
      nL.add(inE);
      clickers.put(vId, nL);
    }
  }

  private void move(Event inE, int vId) {
    if (clickers.containsKey(vId)) {
      buyers.put(vId, clickers.remove(vId));
      buyers.get(vId).add(inE);
    } else {
      buyers.get(vId).add(inE);
    }
  }
}
