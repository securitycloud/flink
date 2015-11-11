/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package flink.Tests;

import flink.Flow;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;

/**
 *
 * @author Lastovicka
 */
public class ScanTest extends RichMapFunction<Flow, String> {

    private static int counter;
    private static final Map<String, Integer> aggregate = new HashMap<>();
    ParameterTool parameterTool;

    private static final ScanTest singleton = new ScanTest();

    private ScanTest() {
        Logger.getLogger(TopNTest.class.getName()).log(Level.INFO, "scan constructor called");
        ScanTest.counter = 0;
        try {
            this.parameterTool = ParameterTool.fromPropertiesFile("/tmp/flink.properties");
            //this.parameterTool = ParameterTool.fromPropertiesFile("src\\main\\resources\\flink.properties");
        } catch (IOException ex) {
            Logger.getLogger(ScanTest.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public static ScanTest getInstance() {
        return singleton;
    }

    public static int getCounter() {
        return counter;
    }

    public static void setCounter(int counter) {
        ScanTest.counter = counter;
    }

    @Override
    public String map(Flow flow) throws Exception {
        if (flow != null) {
            if (flow.getFlags().equals("....S.")) {
                String srcIP = flow.getSrc_ip_addr();
                if (aggregate.containsKey(srcIP)) {
                    int flowsFromMap = aggregate.get(srcIP);
                    aggregate.put(srcIP, flowsFromMap + 1);
                } else {
                    aggregate.put(srcIP, 1);
                }
            }
        }
        counter++;
        if ((counter % parameterTool.getInt("countwindow.size")) == 0) {
            String map = "content:" + aggregate.toString();
            aggregate.clear();
            return "scan " + String.valueOf(System.currentTimeMillis()) + " " + String.valueOf(parameterTool.getInt("countwindow.size")) + " " + map;

        }
        return null;
    }

}
