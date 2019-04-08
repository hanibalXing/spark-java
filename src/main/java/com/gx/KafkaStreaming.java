package com.gx;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class KafkaStreaming {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local[3]").setAppName("wordCount");
        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(5));
        Map<String, Integer> topicThreadMap = new HashMap<String, Integer>();
        topicThreadMap.put("TestTopic",1);
        JavaPairReceiverInputDStream<String, String> lines = KafkaUtils.createStream(jsc,
                "192.168.1.110:2181,192.168.1.111:2181,192.168.1.112:2181",
                "DefaultConsumerGroup", topicThreadMap
        );
        JavaDStream<String> words = lines.flatMap(
                new FlatMapFunction<Tuple2<String, String>, String>() {
                    @Override
                    public Iterable<String> call(Tuple2<String, String> tuple2) throws Exception {
                        return Arrays.asList(tuple2._2.split(" "));
                    }
                }
        );
        JavaPairDStream<String, Integer> pairs = words.mapToPair(
                new PairFunction<String, String, Integer>() {
                    @Override
                    public Tuple2<String, Integer> call(String s) throws Exception {
                        return new Tuple2<String, Integer>(s, 1);
                    }
                }
        );

        JavaPairDStream<String, Integer> contpairs = pairs.reduceByKey(
                new Function2<Integer, Integer, Integer>() {
                    @Override
                    public Integer call(Integer integer, Integer integer2) throws Exception {
                        return integer + integer2;
                    }
                }
        );
        contpairs.print();
        jsc.start();
        jsc.awaitTermination();
        jsc.close();
    }


}
