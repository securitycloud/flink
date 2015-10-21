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
public class CountWindow extends RichMapFunction<String, String> {

    private static int counter;
    ParameterTool parameterTool;
    private static long timeStart;
    private static long timeEnd;
    private static int totalFlows;

    private static final CountWindow singleton = new CountWindow();

    private CountWindow() {
        CountWindow.counter = 0;
        CountWindow.timeStart = Long.MAX_VALUE;
        CountWindow.timeEnd = 0;
        CountWindow.totalFlows = 0;
        try {
            this.parameterTool = ParameterTool.fromPropertiesFile("/tmp/flink.properties");
            //this.parameterTool = ParameterTool.fromPropertiesFile("src\\main\\resources\\flink.properties");
        } catch (IOException ex) {
            Logger.getLogger(CountWindow.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public static CountWindow getInstance() {
        return singleton;
    }

    public static int getCounter() {
        return counter;
    }

    public static void setCounter(int counter) {
        CountWindow.counter = counter;
    }

    @Override
    public String map(String s) throws Exception {
        String[] parts = s.split(" ");
        long timestamp = Long.parseLong(parts[1]);
        //long timestamp = System.currentTimeMillis();
        if (timestamp < timeStart) {
            timeStart = timestamp;
        }
        if (timestamp > timeEnd) {
            timeEnd = timestamp;
        }
        totalFlows += Integer.parseInt(parts[2]);
        long windowLimit = 20l * (long)parameterTool.getInt("countwindow.size");
        if (totalFlows >= windowLimit) {
            long postProcessingTime = System.currentTimeMillis();
            if (timeEnd < postProcessingTime) {
                timeEnd = postProcessingTime;
            }
            long speed = windowLimit / (timeEnd - timeStart); //rychlost v tocich za milisekundu = prumer v tisicich toku za vterinu
            totalFlows = 0;
            CountWindow.timeStart = Long.MAX_VALUE;
            CountWindow.timeEnd = 0;
            return "CountWindow se dopocital na hodnotu " + String.valueOf(windowLimit) + " toku :), prumerna rychlost zpracovani byla " + String.valueOf(speed) + "k toku za vterinu";


        }
        return null;
        //return "Prisla zprava: " + s + ", totalFlows = " + totalFlows;
    }

}
