package flink;

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
import flink.Tests.*;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer082;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.KafkaSink;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.WindowedDataStream;
import org.apache.flink.streaming.api.functions.RichWindowMapFunction;
import org.apache.flink.streaming.api.functions.WindowMapFunction;
import org.apache.flink.streaming.api.windowing.helper.Count;
import org.apache.flink.streaming.api.windowing.helper.Time;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;
import org.apache.flink.util.Collector;

/**
 * Skeleton for a Flink Job.
 *
 * For a full example of a Flink Job, see the WordCountJob.java file in the same
 * package/directory or have a look at the website.
 *
 * You can also generate a .jar file that you can submit on your Flink cluster.
 * Just type mvn clean package in the projects root directory. You will find the
 * jar in target/flink-quickstart-0.1-SNAPSHOT-Sample.jar
 *
 */
public class Job {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        String propertiesFile = "src\\main\\resources\\flink.properties";
        //String propertiesFile = "/tmp/flink.properties";
        ParameterTool parameterTool = ParameterTool.fromPropertiesFile(propertiesFile);

        DataStream<String> messageStream = env.addSource((FlinkKafkaConsumer082) new FlinkKafkaConsumer082<>(parameterTool.getRequired("consumer.topic"), new FlowSchema(), parameterTool.getProperties()));
        //DataStream<String> messageStream = env.addSource(new FlinkKafkaConsumer082<>(parameterTool.getRequired("topic"), new SimpleStringSchema(), parameterTool.getProperties()));

        DataStream<String> messageStream2 = messageStream.copy();
        
//        WindowedDataStream<String> windowedStream = messageStream.window(Count.of(10))
//                                                                 .every(Time.of(1, TimeUnit.SECONDS));
//        windowedStream.flatten().print();
        
        FilterTest filterTest = FilterTest.getInstance();

        
        
        messageStream.rebalance()
                .filter((FilterFunction) filterTest);            
//                .addSink((KafkaSink) new KafkaSink<>(parameterTool.getRequired("bootstrap.servers"),
//                                parameterTool.getRequired("producer.topic"),
//                                new SimpleStringSchema()));
        
        messageStream2.rebalance()
                .map((MapFunction) filterTest)
                .filter((Object t) -> t != null)                
                .addSink((KafkaSink) new KafkaSink<>(parameterTool.getRequired("bootstrap.servers"),
                                parameterTool.getRequired("producer.topic"),
                                new SimpleStringSchema()));     
        
        

        env.execute();
    }

}
