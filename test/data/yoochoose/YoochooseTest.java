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

//  @Test
//  public void buildVwTrainFileTest() {
//
//    YoochooseParser p = new YoochooseParser(9_000_000, 500_000, Format.VW, Mode.TRAIN);
//    p.buildMap("../../data/recsys2015/yoochoose-clicks2.dat", Event.Type.CLICK, ',');
//    p.buildMap("../../data/recsys2015/yoochoose-buys2.dat", Event.Type.PURCHASE, ',');
//    p.analyse();
//
//    // Build the VW file based on our analysis
//    p.output("yoochoose-train-vw.dat");
//  }
  
  @Test
  public void buildLibSvmTrainFileTest() {

    YoochooseParser p = new YoochooseParser(9_000_000, 500_000, Format.LIBSVM, Mode.TRAIN, false);
    p.buildMap("../../data/recsys2015/yoochoose-clicks.dat", Event.Type.CLICK, ',');
    p.buildMap("../../data/recsys2015/yoochoose-buys.dat", Event.Type.PURCHASE, ',');
    p.analyse();

    // Build the VW file based on our analysis
    p.output("yoochoose-train-libsvm.dat");
  }
  
  @Test
  public void buildLibSvmTestFileTest() {

    YoochooseParser p = new YoochooseParser(1_000_000, 1, Format.LIBSVM, Mode.TEST, false);
    p.buildMap("../../data/recsys2015/yoochoose-test.dat", Event.Type.CLICK, ',');
    p.analyse();

    // Build the VW file based on our analysis
    p.output("yoochoose-test-libsvm.dat");
  }

//  @Test
//  public void buildVwTestFileTest() {
//
//    YoochooseParser p = new YoochooseParser(1_000_000, 1, Format.VW, Mode.TEST);
//    p.buildMap("../../data/recsys2015/yoochoose-test.dat", Event.Type.CLICK, ',');
//    p.analyse();
//
//    // Build the VW file based on our analysis
//    p.output("yoochoose-test.vw");
//  }
}
