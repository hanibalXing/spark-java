package com.gx;

import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.util.*;

public class KafkaDirectStreaming {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("wordCount");
        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(5));
        Map<String, String> topicThreadMap = new HashMap<String, String>();
        topicThreadMap.put("metadata.broker.list","192.168.1.110:9092,192.168.1.111:9092,192.168.1.112:9092");
        Set<String> topics=new HashSet<String>();
        topics.add("gxtopic");
        JavaPairInputDStream<String, String> directStream = KafkaUtils.createDirectStream(jsc, String.class, String.class, StringDecoder.class, StringDecoder.class, topicThreadMap, topics);

        JavaDStream<String> words=directStream.flatMap(
                new FlatMapFunction<Tuple2<String, String>, String>() {
                    @Override
                    public Iterable<String> call(Tuple2<String, String> tuple) throws Exception {
                        return Arrays.asList(tuple._2.split(" "));
                    }
                }
        );

        JavaPairDStream<String, Integer> pairs= words.mapToPair(
                new PairFunction<String, String, Integer>() {
                    @Override
                    public Tuple2<String, Integer> call(String s) throws Exception {
                        return new Tuple2<String,Integer>(s,1);
                    }
                }
        );
        JavaPairDStream<String, Integer> count=pairs.reduceByKey(

                new Function2<Integer, Integer, Integer>() {
                    @Override
                    public Integer call(Integer integer, Integer integer2) throws Exception {
                        return integer+integer2;
                    }
                }
        );
        count.print();
        jsc.start();
        jsc.awaitTermination();
        jsc.close();


    }
}
