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
public class EmptyTest extends RichMapFunction<Flow, String>{

    private static int counter;
    ParameterTool parameterTool;

    private static final EmptyTest singleton = new EmptyTest();

    private EmptyTest() {
        Logger.getLogger(EmptyTest.class.getName()).log(Level.SEVERE, "Filter constructor called!!!!!!!!!!!!!!!!!!!!!!");
        EmptyTest.counter = 0;
        try {
            this.parameterTool = ParameterTool.fromPropertiesFile("/tmp/flink.properties");
            //this.parameterTool = ParameterTool.fromPropertiesFile("src\\main\\resources\\flink.properties");
        } catch (IOException ex) {
            Logger.getLogger(EmptyTest.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public static EmptyTest getInstance() {
        return singleton;
    }
    
    public static int getCounter() {
        return counter;
    }

    public static void setCounter(int counter) {
        EmptyTest.counter = counter;
    }

    @Override
    public String map(Flow flow) throws Exception {
        counter++;
        if ((counter % parameterTool.getInt("countwindow.size")) == 0) {
            return "empty " + String.valueOf(System.currentTimeMillis()) + " " + String.valueOf(parameterTool.getInt("countwindow.size")) + " #" + getRuntimeContext().getIndexOfThisSubtask();
        }
        return null;
    }

}
