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
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer082;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.KafkaSink;
import org.apache.flink.api.common.functions.MapFunction;

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

        String testMethod;
        if (args.length == 1) {
            testMethod = args[0];
        } else {
            testMethod = "empty";
        }

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //String propertiesFile = "src\\main\\resources\\flink.properties";
        String propertiesFile = "/tmp/flink.properties";
        ParameterTool parameterTool = ParameterTool.fromPropertiesFile(propertiesFile);
        ParameterTool countWindowParams = ParameterTool.fromPropertiesFile("/tmp/flink_count_window.properties");

        DataStream<String> messageStream = env.addSource((FlinkKafkaConsumer082) new FlinkKafkaConsumer082<>(parameterTool.getRequired("consumer.topic"), new FlowSchema(), parameterTool.getProperties()));
        DataStream<String> serviceStream = env.addSource((FlinkKafkaConsumer082) new FlinkKafkaConsumer082<>(countWindowParams.getRequired("consumer.topic"), new SimpleStringSchema(), countWindowParams.getProperties()));

        CountWindow countWindow = CountWindow.getInstance();
        MapFunction test = null;

        switch (testMethod) {
            case "empty":
                test = EmptyTest.getInstance();
                break;
            case "filter":
                test = FilterTest2.getInstance();
                break;
            case "count":
                test = CountTest.getInstance();
                break;
            case "aggregate":
                test = AggregateTest.getInstance();
                break;
            case "topN":
                test = TopNTest.getInstance();
                break;
            case "scan":
                test = ScanTest.getInstance();
                break;
            default:
                throw new UnsupportedOperationException("Unknown method " + testMethod);
        }
        messageStream.map(test)
                .filter((Object t) -> t != null)
                .addSink((KafkaSink) new KafkaSink<>(parameterTool.getRequired("bootstrap.servers"),
                                parameterTool.getRequired("producer.topic"),
                                new SimpleStringSchema()));

        serviceStream.map((MapFunction) countWindow)
                .filter((Object t) -> t != null)
                .addSink((KafkaSink) new KafkaSink<>(countWindowParams.getRequired("bootstrap.servers"),
                                countWindowParams.getRequired("producer.topic"),
                                new SimpleStringSchema()));
        env.execute();

    }

}
