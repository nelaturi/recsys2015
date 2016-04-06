package data.yoochoose;


import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Ignore;
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
  public void buildVwTrainFileTest() {

    Map<Integer, List<Event>> visitors = new HashMap<>(3_000_000);
    YoochooseParser p = new YoochooseParser(); 
    p.buildMap(visitors, "../../data/recsys2015/yoochoose-clicks.dat", Event.Type.CLICK, ',');
    p.buildMap(visitors, "../../data/recsys2015/yoochoose-buys.dat", Event.Type.PURCHASE, ',');
    p.analyse(visitors);

    // Build the VW file based on our analysis
    p.buildSaveVwFile(visitors, "yoochoose-train.vw", true);
  }
  
  @Test
  public void buildVwTestFileTest() {

    Map<Integer, List<Event>> visitors = new HashMap<>(1_000_000);
    YoochooseParser p = new YoochooseParser(); 
    p.buildMap(visitors, "../../data/recsys2015/yoochoose-test.dat", Event.Type.CLICK, ',');
    p.analyse(visitors);

    // Build the VW file based on our analysis
    p.buildSaveVwFile(visitors, "yoochoose-test.vw", false);
  }
}
