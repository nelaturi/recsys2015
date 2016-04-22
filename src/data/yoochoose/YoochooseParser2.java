package data.yoochoose;

import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

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
public class YoochooseParser2 {

  /**
   * Exists only to be re-used to reduce GCing.
   */
  private Set<Integer> uniques;

  /**
   * Exists only to be re-used to reduce GCing.
   */
  private Map<Integer, Integer> similar;

  private static final String BUYER_LABEL = "1";

  private static final String CLICKER_LABEL = "0";

  private static final int NUM_EVENTS = 400;

  private static final String VW_DELIMITER = "|";

  private int labelCounter;

  private static final String FEAT_SEP = " ";

  private static final String FEAT_VAL_SEP = ":";

  private static final Logger LOG = LoggerFactory.getLogger(YoochooseParser2.class);

  private static final long LOG_INTERVAL = 5_000L;

  private Map<String, Integer> labelMappings;

  private Map<Integer, Item> items;

  private Map<Integer, Integer> mostPopularItems;

  private Map<Integer, Integer> mostPopularCategories;

  /**
   * Train or testing - testing files only differ in not having a class label set.
   */
  private final Mode mode;

  /**
   * The output file format.
   */
  private final Format format;

  private Map<Integer, Session> sessions;

  private DateTimeFormatter dtf;

  public YoochooseParser2(Format inF, Mode inM) {
    format = inF;
    mode = inM;
    String format = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";
    dtf = DateTimeFormatter.ofPattern(format);
    sessions = new HashMap<>();
    labelMappings = new HashMap<>();
    items = new HashMap<>();
    similar = new HashMap<>();
    uniques = new HashSet<>();
    mostPopularItems = new HashMap<>();
    mostPopularCategories = new HashMap<>();
  }

  /**
   * Handles the clicks, buys, test and solution *.dat files in the yoochoose 7z file.
   * 
   * @param inFname
   * @param inT
   */
  public void load(String inFname, Event.Type inT, char inSeparatorChar) {
    LOG.info("Loading {}", inFname);
    int total = 0, current = 0;
    String[] nextLine = null;
    long currTime = 0L;
    long startTime1 = System.currentTimeMillis();
    try (CSVReader reader = new CSVReader(new FileReader(inFname), inSeparatorChar)) {
      while ((nextLine = reader.readNext()) != null) {
        processEvent(nextLine);
        total++;

        currTime = System.currentTimeMillis();
        if ((currTime - startTime1) > LOG_INTERVAL) {
          LOG.info("{} events processed (rate: {}/sec)", total,
              (float) ((total - current) / LOG_INTERVAL) * 1_000);
          startTime1 = currTime;
          current = total;
        }
      }
      LOG.info("{} total events processed", total);
    } catch (Exception ie) {
      LOG.error("Error at line {} in file", total, ie);
      LOG.error("Complete line is: '{},{},{},{}'", nextLine[0], nextLine[1], nextLine[2],
          nextLine[3]);
    }
  }

  private void processEvent(String[] nextLine) {
    int vId = Integer.parseInt(nextLine[0]);
    LocalDateTime dt = LocalDateTime.parse(nextLine[1], dtf);
    Session currS = null;
    if (!sessions.containsKey(vId)) {
      currS = new Session();
      sessions.put(vId, currS);
    }
    currS = sessions.get(vId);
    Event e = new Event();
    Item i = new Item();
    e.setDate(dt);
    e.setItemId(Integer.parseInt(nextLine[2]));
    i.setId(e.getItemId());

    // We're handling a click, so just set category
    if (nextLine.length == 4) {
      int catId = 0;
      if ("S".equals(nextLine[3])) {
        catId = 27;
      } else {
        catId = Integer.parseInt(nextLine[3]);
      }
      e.setCategoryId(catId);
      i.setCategoryId(catId);
    } else {
      // We're handling a purchase, set price and quantity
      e.setPrice(Integer.parseInt(nextLine[3]));
      i.setPrice(e.getPrice());
      i.setPurchased(true);
      e.setQuantity(Integer.parseInt(nextLine[4]));
      if (e.getQuantity() > 1) {
        i.setMultiPurchase(true);
      }
      // Flag session as a buying session if not done already
      if (!currS.isPurchaser()) {
        currS.setPurchaser(true);
      }

      // Lastly, make sure all items in this session are flagged as "seen with purchased"
      for (Event sessEvent : currS.getEvents()) {
        Item associatedItem = items.get(sessEvent.getItemId());
        if (associatedItem != null && !items.get(sessEvent.getItemId()).isSeenWithPurchased()) {
          associatedItem.setSeenWithPurchased(true);
        }
      }
    }
    putOrUpdate(i);

    // Add the constructed event to the session
    currS.add(e);
  }

  /**
   * Either update an existing item or add a brand new, unseen one.
   * 
   * @param inJustReadItem
   */
  private void putOrUpdate(Item inJustReadItem) {
    if (!items.containsKey(inJustReadItem.getId())) {
      // Simplest case - new item never seen before
      items.put(inJustReadItem.getId(), inJustReadItem);
    } else {
      // We need to evaluate each field to see if we have better data now (promote from "unknown" to
      // "known")
      Item currI = items.get(inJustReadItem.getId());
      if (currI.getCategoryId() == 0 && inJustReadItem.getCategoryId() > 0) {
        currI.setCategoryId(inJustReadItem.getCategoryId());
      }
      if (inJustReadItem.getPrice() > 0 && currI.getPrice() != inJustReadItem.getPrice()) {
        // int currPrice = currI.getPrice();
        currI.setPrice(inJustReadItem.getPrice());
      }
      if (inJustReadItem.isPurchased() && !currI.isPurchased()) {
        currI.setPurchased(true);
      }
      if (inJustReadItem.isMultiPurchase() && !currI.isMultiPurchase()) {
        currI.setMultiPurchase(true);
      }
    }
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
      for (Map.Entry<Integer, Session> clickerEntry : sessions.entrySet()) {
        writeSession(clickerEntry, mainFile, format, mode);
        writeLabel(clickerEntry.getKey(), labelsFile);
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
      LOG.error("Error writing file", e);
    }
  }

  private void writeLabel(Integer inSessionId, PrintWriter out2) {
    StringBuilder sb = new StringBuilder();
    sb.append(inSessionId + "\n");
    out2.write(sb.toString());
  }


  private void writeSession(Entry<Integer, Session> entry, PrintWriter out, Format inF, Mode inM) {
    List<Event> events = entry.getValue().getEvents();
    events.sort(null);

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
    for (int eventCtr = 0; eventCtr < eLimit; eventCtr++) {
      Event e = events.get(eventCtr);
      Event nextE = null;
      if (eventCtr < events.size() - 1) {
        nextE = events.get(eventCtr + 1);
      }

      long duration = 100L;
      if (nextE != null) {
        duration = calculateDuration(e, nextE);
      }

      LocalDateTime ldt = e.getDate();
      String prefix = "";
      if (Format.VW.equals(inF)) {
        sb.append("|Event" + eventCtr + FEAT_SEP);
      } else {
        prefix += "Event" + eventCtr + FEAT_SEP;
      }
      append(sb, prefix + "mth", ldt.getMonthValue());
      append(sb, prefix + "day", ldt.getDayOfMonth());
      append(sb, prefix + "hour", ldt.getHour());
      append(sb, prefix + "minute", ldt.getMinute());
      append(sb, prefix + "second", ldt.getSecond());
      append(sb, prefix + e.getItemId() + "-itemId", 1);
      append(sb, prefix + e.getItemId() + "item-was-purchased",
          wasPurchased(e.getItemId()) ? 1 : 0);
      append(sb, prefix + e.getItemId() + "item-was-multi-purchase",
          wasMultiPurchase(e.getItemId()) ? 1 : 0);
      append(sb, prefix + e.getItemId() + "item-price", itemPrice(e.getItemId()));
      append(sb, prefix + "dwellTime", duration);
      if (e instanceof Click) {
        Click c = (Click) e;
        append(sb, prefix + c.getCategoryId() + "-catId", 1);
        append(sb, prefix + "special", c.isSpecial() ? 1 : 0);
        append(sb, prefix + "category-simplified", simplifyCategory(c));
      }
    }
    return sb;
  }

  private void append(StringBuilder sb, String inL, Object inValue) {
    sb.append(FEAT_SEP + mapLabel(inL) + FEAT_VAL_SEP + inValue.toString());
  }

  private int itemPrice(int itemId) {
    int price = 0;
    if (items.containsKey(itemId)) {
      price = items.get(itemId).getPrice();
    }
    return price;
  }

  private boolean wasMultiPurchase(int itemId) {
    Item i = items.get(itemId);
    return i.isMultiPurchase();
  }

  private boolean wasPurchased(int itemId) {
    Item i = items.get(itemId);
    if (i == null) {
      LOG.info("{} not in items registry", itemId);
    }
    return i.isPurchased();
  }


  public void analyse() {
    LOG.info("{} sessions loaded", sessions.size());
    long itemCount = items.size();
    LOG.info("{} items loaded", items.size());
    long pItems = items.values().parallelStream().filter(i -> i.isPurchased()).count();
    LOG.info("{} purchased items", pItems);

    long unpItems = itemCount - pItems;
    LOG.info("{} unpurchased items", unpItems);

    long mpItems = items.values().parallelStream().filter(i -> i.isMultiPurchase()).count();
    LOG.info("{} multi-purchase items", mpItems);

    int minPrice = items.values().parallelStream().mapToInt(i -> i.getPrice()).min().getAsInt();
    LOG.info("item min price: {}", minPrice);
    int maxPrice = items.values().parallelStream().mapToInt(i -> i.getPrice()).max().getAsInt();
    LOG.info("item max price: {}", maxPrice);
    double avgPrice =
        items.values().parallelStream().mapToInt(i -> i.getPrice()).average().getAsDouble();
    LOG.info("item avg price: {}", avgPrice);

    Map<Integer, List<Item>> grouped = items.values().parallelStream()
        .collect(Collectors.groupingByConcurrent(i -> range(i.getPrice())));
    for (Map.Entry<Integer, List<Item>> entry : grouped.entrySet()) {
      int minPrice2 = entry.getValue().stream().mapToInt(i -> i.getPrice()).min().getAsInt();
      int maxPrice2 = entry.getValue().stream().mapToInt(i -> i.getPrice()).max().getAsInt();
      LOG.info("{}:{} (min={}, max={})", entry.getKey(), entry.getValue().size(), minPrice2,
          maxPrice2);
    }

    long swpItems = items.values().parallelStream().filter(i -> i.isSeenWithPurchased()).count();
    LOG.info("{} seen with purchased items", swpItems);

    long itemsWithPricesCount =
        items.values().parallelStream().filter(i -> i.getPrice() > 0).count();
    LOG.info("{} items with prices", itemsWithPricesCount);
  }

  private int range(int inPrice) {
    if (inPrice == 0){
      return 0;
    }
    
    int[] wayPoints = {1,100,500,1_000,2_000,5_000,10_000,50_000,100_000,350_000};
    
    for (int i = 0; i<wayPoints.length;i++ ){
      if (inPrice > wayPoints[i] && inPrice <= wayPoints[i + 1]){
        return ++i;
      }
    }
    //TODO make better
    LOG.info("AAAAGGGGGGGGGGGGHHHHHHHHHHHHHHHHHHHHHHHHHHHH");
    return -1;
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
        LOG.error("Unsupported format {} requested for {} label", format, inL);
        return "";
    }
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
    append(sb, "viewedPopularItems", didViewPopular(events, mostPopularItems) ? 1.0 : 0.0);

    // Rough approximation for popular, purchased categories
    append(sb, "viewedPopularCats", didViewPopular(events, mostPopularCategories) ? 1.0 : 0.0);

    // Rough approximation for content similarity by category
    append(sb, "catSimilarity", prevalantCategory(events));

    return sb;
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

  /**
   * Returns the category seen most often in a session.
   * 
   * @param events
   * @return
   */
  private int prevalantCategory(List<Event> events) {
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
        // This will be the highest frequency of category
        return entry.getValue();
      }
    }
    return rVal;
  }

  private boolean didViewPopular(List<Event> events, Map<Integer, Integer> inMostPopular) {
    for (Event e : events) {
      if (inMostPopular.containsValue(e.getItemId())) {
        return true;
      }
    }
    return false;
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
}
