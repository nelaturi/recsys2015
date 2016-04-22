package data.yoochoose;


import org.junit.Test;

import data.yoochoose.Event;
import data.yoochoose.YoochooseParser;

/**
 * Test for {@link YoochooseParser}.
 * 
 * @author hsheil
 */
public class YoochooseTest2 {

  @Test
  public void buildLibSvmTrainFileTest() {

    YoochooseParser2 p = new YoochooseParser2(Format.LIBSVM, Mode.TRAIN);
    p.load("../../data/recsys2015/yoochoose-clicks.dat", Event.Type.CLICK, ',');
    p.load("../../data/recsys2015/yoochoose-buys.dat", Event.Type.PURCHASE, ',');
    p.analyse();

    // Build the VW file based on our analysis
    p.output("yoochoose-train-libsvm.dat");
  }
}
