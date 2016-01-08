package data.yoochoose;


import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import data.yoochoose.Event;
import data.yoochoose.YoochooseParser;

/**
 * Test for {@link YoochooseParser}.
 * 
 * @author hsheil
 */
public class YoochooseTest {
  
  @Test
  public void buildVwFileTest() {

    Map<Integer, List<Event>> visitors = new HashMap<>(3_000_000);
    YoochooseParser p = new YoochooseParser(); 
    p.buildMap(visitors, "yoochoose-clicks.dat", Event.Type.CLICK, ',');
    p.buildMap(visitors, "yoochoose-buys.dat", Event.Type.PURCHASE, ',');
    p.analyse(visitors);

    // Build the VW file based on our analysis
    p.buildSaveVwFile(visitors, "yoochoose-train.vw");
  }
}
