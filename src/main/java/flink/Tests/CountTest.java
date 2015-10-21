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
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;

/**
 *
 * @author Lastovicka
 */
public class CountTest extends RichFilterFunction<Flow> implements RichFunction, MapFunction<Flow, String> {

    private ParameterTool parameterTool;
    private IntCounter totalFlows;
    private IntCounter packets;

    public CountTest() {
        Logger.getLogger(CountTest.class.getName()).log(Level.SEVERE, "Filter constructor called!!!!!!!!!!!!!!!!!!!!!!");  
        totalFlows = new IntCounter();
        packets = new IntCounter();        
        try {
            this.parameterTool = ParameterTool.fromPropertiesFile("/tmp/flink.properties");
            //this.parameterTool = ParameterTool.fromPropertiesFile("src\\main\\resources\\flink.properties");
        } catch (IOException ex) {
            Logger.getLogger(FilterTest.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    @Override
    public boolean filter(Flow flow) throws Exception {
        this.totalFlows.add(1);
        if (flow == null || flow.getSrc_ip_addr() == null) {
            return false;
        }
        //if (flow.getSrc_ip_addr().equals(parameterTool.getRequired("filter.srcip"))){
        if (flow.getDst_port() == parameterTool.getInt("filter.dstport")) {
            this.packets.add(1);
            return true;
        }
        return false;
    }
    
    @Override
    public String map(Flow t) throws Exception {
        int counter = this.totalFlows.getLocalValue();
        int packetsCounter = this.packets.getLocalValue();
        if ((counter % parameterTool.getInt("countwindow.size")) == 0 || counter == 1) {
            Logger.getLogger(FilterTest.class.getName()).log(Level.SEVERE, "Count window triggered at time {0}, counter value is {1}, filtered: {2}"
                    , new Object[]{System.currentTimeMillis(), packetsCounter});
            return "Index " + getRuntimeContext().getIndexOfThisSubtask() + ": Count window triggered at time " + System.currentTimeMillis() + ", counter value is " + counter + ", filtered: " + packetsCounter;
        }
        return null;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        getRuntimeContext().addAccumulator("totalFlows", totalFlows);
        getRuntimeContext().addAccumulator("packets", packets);
        super.open(parameters); //To change body of generated methods, choose Tools | Templates.
    }
    
    
    
}
