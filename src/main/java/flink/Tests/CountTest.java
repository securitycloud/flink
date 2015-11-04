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
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.functions.RichFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;

/**
 *
 * @author Lastovicka
 */
public class CountTest extends RichMapFunction<Flow, String> {

    private ParameterTool parameterTool;
    private static int counter;
    private static int packets;

    private static final CountTest singleton = new CountTest();
    
    private CountTest() {
        Logger.getLogger(CountTest.class.getName()).log(Level.SEVERE, "Count constructor called");  
        CountTest.counter = 0;      
        CountTest.packets = 0;
        try {
            this.parameterTool = ParameterTool.fromPropertiesFile("/tmp/flink.properties");
            //this.parameterTool = ParameterTool.fromPropertiesFile("src\\main\\resources\\flink.properties");
        } catch (IOException ex) {
            Logger.getLogger(CountTest.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    public static CountTest getInstance() {
        return singleton;
    }
    
   
    @Override
    public String map(Flow flow) throws Exception {
        if (flow != null && flow.getDst_port() == parameterTool.getInt("filter.dstport")) {
            packets += flow.getPackets();
        }
        counter++;
        if ((counter % parameterTool.getInt("countwindow.size")) == 0 || counter == 1) {
            //return "Index " + getRuntimeContext().getIndexOfThisSubtask() + ": Count window triggered at time " + System.currentTimeMillis() + ", counter value is " + counter + ", filtered: " + packetsCounter;
            return "count " + String.valueOf(System.currentTimeMillis()) + " " + String.valueOf(parameterTool.getInt("countwindow.size") + " " + packets);
        }
        return null;
    }
    
}
