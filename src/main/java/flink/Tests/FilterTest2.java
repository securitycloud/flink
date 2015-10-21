/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package flink.Tests;

import flink.Flow;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;

/**
 *
 * @author Lastovicka
 */
public class FilterTest2 extends RichMapFunction<Flow, String>{

    private static int counter;
    private static int filtered;
    ParameterTool parameterTool;

    private static final FilterTest2 singleton = new FilterTest2();

    private FilterTest2() {
        Logger.getLogger(FilterTest2.class.getName()).log(Level.SEVERE, "Filter constructor called!!!!!!!!!!!!!!!!!!!!!!");
        FilterTest2.counter = 0;
        FilterTest2.filtered = 0;
        try {
            this.parameterTool = ParameterTool.fromPropertiesFile("/tmp/flink.properties");
            //this.parameterTool = ParameterTool.fromPropertiesFile("src\\main\\resources\\flink.properties");
        } catch (IOException ex) {
            Logger.getLogger(FilterTest2.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public static FilterTest2 getInstance() {
        return singleton;
    }
    
    public static int getCounter() {
        return counter;
    }

    public static void setCounter(int counter) {
        FilterTest2.counter = counter;
    }

    @Override
    public String map(Flow flow) throws Exception {
        if (flow != null && flow.getDst_port() == parameterTool.getInt("filter.dstport")) {
            filtered++;
        }
        counter++;
        if ((counter % parameterTool.getInt("countwindow.size")) == 0) {
            return "filter " + String.valueOf(System.currentTimeMillis()) + " " + String.valueOf(parameterTool.getInt("countwindow.size") + " " + String.valueOf(filtered));
            }
        return null;
    }

}
