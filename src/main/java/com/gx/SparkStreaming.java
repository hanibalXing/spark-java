package com.gx;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

public class SparkStreaming
{
    public static void main(String[] args) throws InterruptedException {


        SparkConf conf=new SparkConf().setMaster("local[3]").setAppName("wordCount");
        JavaStreamingContext jsc=new JavaStreamingContext(conf, Durations.seconds(5));
        JavaReceiverInputDStream<String> lines=jsc.socketTextStream("120.77.236.231",9999);
      // JavaDStream<String> lines=jsc.textFileStream("C:\\Users\\pc\\Desktop\\sparkfile");
        JavaDStream<String> words=lines.flatMap(
                new FlatMapFunction<String, String>() {
                    @Override
                    public Iterable<String> call(String s) throws Exception {
                        return Arrays.asList(s.split(" "));
                    }
                }
        );
        JavaPairDStream<String,Integer> pairs=words.mapToPair(
                new PairFunction<String, String, Integer>() {
                    @Override
                    public Tuple2<String, Integer> call(String s) throws Exception {
                        return new Tuple2<String, Integer>(s,1);
                    }
                }
        );

        JavaPairDStream<String,Integer> contpairs=pairs.reduceByKey(
                new Function2<Integer, Integer, Integer>() {
                    @Override
                    public Integer call(Integer integer, Integer integer2) throws Exception {
                        return integer+integer2;
                    }
                }
        );
        contpairs.print();



        jsc.start();
        jsc.awaitTermination();
        jsc.close();

    }
}
