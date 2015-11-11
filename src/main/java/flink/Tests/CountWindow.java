/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package flink.Tests;

import flink.Flow;
import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
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
    private static long windowLimit; 
    private static Map<String, Integer> top;
    private static int packets;
    private static int filtered;
    private static boolean isFirst;

    private static final CountWindow singleton = new CountWindow();

    private CountWindow() {
        CountWindow.counter = 0;
        CountWindow.timeStart = Long.MAX_VALUE;
        CountWindow.timeEnd = 0;
        CountWindow.totalFlows = 0;
        CountWindow.packets = 0;
        CountWindow.filtered = 0;
        CountWindow.top = new HashMap<>();
        CountWindow.isFirst = true;
                
        try {
            this.parameterTool = ParameterTool.fromPropertiesFile("/tmp/flink.properties");
            CountWindow.windowLimit = (long)parameterTool.getInt("countwindow.limit");
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
        if (isFirst) {
            timeStart = System.currentTimeMillis();
            isFirst = false;
        }
        //if (timestamp > timeEnd) {
        //    timeEnd = timestamp;
        //}
        totalFlows += Integer.parseInt(parts[2]);
        
////////////////////////////////////////          EMPTY FRAMEWORK          //////////////////////////////////////// 
            if (parts[0].equals("empty")) {
            }

////////////////////////////////////////          TEST FILTER          //////////////////////////////////////// 
            if (parts[0].equals("filter")) {
                filtered += Integer.parseInt(parts[3]);
            }

////////////////////////////////////////          TEST COUNT          //////////////////////////////////////// 
            if (parts[0].equals("count")) {
                packets += Integer.parseInt(parts[3]);
            }

////////////////////////////////////////          TEST AGGREGATE          //////////////////////////////////////// 
            if (parts[0].equals("aggregate")) {
                for (String field : parts) {
                    String[] divided = field.split("=");
                    if (divided.length > 1) {
                        String IP = divided[0];
                        if (IP.charAt(0) == '{') {
                            IP = IP.substring(1);
                        }
                        int packetsCount = Integer.parseInt(divided[1].substring(0, divided[1].length() - 1));
                        if (top.containsKey(IP)) {
                            int packetsFromMap = top.get(IP);
                            top.put(IP, packetsFromMap + packetsCount);
                        } else {
                            top.put(IP, packetsCount);
                        }
                    }
                }
                if (totalFlows >= windowLimit) {
                    Iterator<String> it = top.keySet().iterator();
                    StringBuilder sb = new StringBuilder();
                    while (it.hasNext()) {
                        String key = it.next();
                        sb.append(key).append(" ").append(String.valueOf(top.get(key))).append(", ");
                    }
                }
            }

////////////////////////////////////////          TEST TOP N          //////////////////////////////////////// 
            if (parts[0].equals("topn")) {
                for (String field : parts) {
                    String[] divided = field.split("=");
                    if (divided.length > 1) {
                        String IP = divided[0];
                        if (IP.charAt(0) == '{') {
                            IP = IP.substring(1);
                        }
                        int packetsCount = Integer.parseInt(divided[1].substring(0, divided[1].length() - 1));
                        if (top.containsKey(IP)) {
                            int packetsFromMap = top.get(IP);
                            top.put(IP, packetsFromMap + packetsCount);
                        } else {
                            top.put(IP, packetsCount);
                        }
                    }
                }
                if (totalFlows == windowLimit) {
                    ValueComparator bvc = new ValueComparator(top);
                    TreeMap<String, Integer> sorted = new TreeMap<>(bvc);
                    sorted.putAll(top);
                    Iterator<String> it = sorted.keySet().iterator();
                    int i = 1;
                    StringBuilder sb = new StringBuilder();
                    while (it.hasNext()) {
                        String key = it.next();
                        sb.append(String.valueOf(i)).append(" ").append(key).append(" ").append(String.valueOf(top.get(key))).append(", ");
                        i++;
                        if (i > 10) {
                            break;
                        }

                    }
                }
            }

////////////////////////////////////////          TEST SYN SCAN          //////////////////////////////////////// 
            if (parts[0].equals("scan")) {
                for (String field : parts) {
                    String[] divided = field.split("=");
                    if (divided.length > 1) {
                        String IP = divided[0];
                        if (IP.charAt(0) == '{') {
                            IP = IP.substring(1);
                        }
                        int packetsCount = Integer.parseInt(divided[1].substring(0, divided[1].length() - 1));
                        if (top.containsKey(IP)) {
                            int packetsFromMap = top.get(IP);
                            top.put(IP, packetsFromMap + packetsCount);
                        } else {
                            top.put(IP, packetsCount);
                        }
                    }
                }
                if (totalFlows == windowLimit) {
                    ValueComparator bvc = new ValueComparator(top);
                    TreeMap<String, Integer> sorted = new TreeMap<>(bvc);
                    sorted.putAll(top);
                    Iterator<String> it = sorted.keySet().iterator();
                    int i = 1;
                    StringBuilder sb = new StringBuilder();
                    while (it.hasNext()) {
                        String key = it.next();
                        sb.append(String.valueOf(i)).append(" ").append(key).append(" ").append(String.valueOf(top.get(key))).append(", ");
                        i++;
                        if (i > 100) {
                            break;
                        }
                    }
                }
            }

        
        if (totalFlows >= windowLimit) {
            long postProcessingTime = System.currentTimeMillis();
            if (timeEnd < postProcessingTime) {
                timeEnd = postProcessingTime;
            }
            long speed = windowLimit / (timeEnd - timeStart); //rychlost v tocich za milisekundu = prumer v tisicich toku za vterinu
            cleanVars();
            return parts[0] + ": CountWindow #" + getRuntimeContext().getIndexOfThisSubtask() + " se dopocital na hodnotu " + String.valueOf(windowLimit) + " toku :), prumerna rychlost zpracovani byla " + String.valueOf(speed) + "k toku za vterinu";


        }
        return null;
        //return "Prisla zprava: " + s + ", totalFlows = " + totalFlows;
    }
 private void cleanVars() {
        totalFlows = 0;
        packets = 0;
        filtered = 0;
        top.clear();
        timeStart = Long.MAX_VALUE;
        timeEnd = 0;
        isFirst = true;
    }
}


class ValueComparator implements Comparator<String> {

    Map<String, Integer> base;

    public ValueComparator(Map<String, Integer> base) {
        this.base = base;
    }

    @Override
    public int compare(String a, String b) {
        if (base.get(a) >= base.get(b)) {
            return -1;
        } else {
            return 1;
        }
    }
}
