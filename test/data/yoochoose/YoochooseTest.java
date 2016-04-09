package data.yoochoose;


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

    YoochooseParser p = new YoochooseParser(9_000_000, 500_000); 
    p.buildMap("../../data/recsys2015/yoochoose-clicks.dat", Event.Type.CLICK, ',');
    p.buildMap("../../data/recsys2015/yoochoose-buys.dat", Event.Type.PURCHASE, ',');
    p.analyse();

    // Build the VW file based on our analysis
    p.buildSaveVwFile("yoochoose-train.vw", true);
  }
  
  @Test
  public void buildVwTestFileTest() {

    YoochooseParser p = new YoochooseParser(1_000_000, 1); 
    p.buildMap("../../data/recsys2015/yoochoose-test.dat", Event.Type.CLICK, ',');
    p.analyse();

    // Build the VW file based on our analysis
    p.buildSaveVwFile("yoochoose-test.vw", false);
  }
}
