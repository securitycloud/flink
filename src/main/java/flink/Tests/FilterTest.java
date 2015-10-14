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
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.utils.ParameterTool;

/**
 *
 * @author Lastovicka
 */
public class FilterTest implements FilterFunction<Flow>, MapFunction<Flow, String> {

    private static int counter;
    private static int filtered;
    ParameterTool parameterTool;

    private static final FilterTest singleton = new FilterTest();

    private FilterTest() {
        Logger.getLogger(FilterTest.class.getName()).log(Level.SEVERE, "Filter constructor called!!!!!!!!!!!!!!!!!!!!!!");
        FilterTest.counter = 0;
        FilterTest.filtered = 0;
        try {
            //this.parameterTool = ParameterTool.fromPropertiesFile("/tmp/flink.properties");
            this.parameterTool = ParameterTool.fromPropertiesFile("src\\main\\resources\\flink.properties");
        } catch (IOException ex) {
            Logger.getLogger(FilterTest.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public static FilterTest getInstance() {
        return singleton;
    }

    @Override
    public boolean filter(Flow flow) throws Exception {
        if (flow == null || flow.getSrc_ip_addr() == null) {
            return false;
        }
        //if (flow.getSrc_ip_addr().equals(parameterTool.getRequired("filter.srcip"))){
        if (flow.getDst_port() == parameterTool.getInt("filter.dstport")) {
            //if( (counter % parameterTool.getInt("countwindow.size") ) == 0){
            filtered++;
            return true;
            //}
        }
        return false;
    }

    public static int getCounter() {
        return counter;
    }

    public static void setCounter(int counter) {
        FilterTest.counter = counter;
    }

    @Override
    public String map(Flow t) throws Exception {
        //setCounter(counter + 1);
        counter++;
        if ((counter % parameterTool.getInt("countwindow.size")) == 0 || counter == 1) {
            Logger.getLogger(FilterTest.class.getName()).log(Level.SEVERE, "Count window triggered at time {0}, counter value is {1}, filtered: {2}"
                    , new Object[]{System.currentTimeMillis(), counter, filtered});
            return "Count window triggered at time " + System.currentTimeMillis() + ", counter value is " + counter + ", filtered: " + filtered;
        }
        return null;
    }

}
