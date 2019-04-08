package com.gx;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.List;

public class WindowHotSearch
{

    public static void main(String[] args)
    {
        SparkConf conf=new SparkConf().setMaster("local[2]").setAppName("wordCount");
        JavaStreamingContext jsc=new JavaStreamingContext(conf, Durations.seconds(2));
        JavaReceiverInputDStream<String> searchLogDStream=jsc.socketTextStream("192.168.1.2",9999);
        JavaDStream<String> searchWordDStream =searchLogDStream.map(new Function<String, String>() {
            @Override
            public String call(String s) throws Exception {
                return s.split(" ")[1];
            }
        });

        JavaPairDStream<String,Integer> spd=searchWordDStream.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String,Integer>(s,1);
            }
        });

        //针对spd做滑动窗口操作,没隔10秒统计最近60秒的搜索词
        JavaPairDStream<String,Integer> countsStream=spd.reduceByKeyAndWindow(
                new Function2<Integer, Integer, Integer>() {
                    @Override
                    public Integer call(Integer integer, Integer integer2) throws Exception {
                        return integer+integer2;
                    }
                },Durations.seconds(60),Durations.seconds(10)
        );
        //这个transformToPair相当于一个合并操作，把几个rdd的操作合并成1个Dstream
        JavaPairDStream<String,Integer> finalDStream=
        countsStream.transformToPair(
                new Function<JavaPairRDD<String, Integer>, JavaPairRDD<String, Integer>>() {
                    @Override
                    public JavaPairRDD<String, Integer> call(JavaPairRDD<String, Integer> stringIntegerJavaPairRDD) throws Exception {
                        JavaPairRDD<Integer,String> countSearcheRDD=stringIntegerJavaPairRDD.mapToPair(
                                new PairFunction<Tuple2<String, Integer>, Integer, String>() {
                                    @Override
                                    public Tuple2<Integer, String> call(Tuple2<String, Integer> tup) throws Exception {
                                        return new Tuple2<Integer, String>(tup._2,tup._1);
                                    }
                                }
                        );
                        JavaPairRDD<Integer, String> integerStringJavaPairRDD = countSearcheRDD.sortByKey(false);
                        JavaPairRDD<String, Integer> finalPRDD=integerStringJavaPairRDD.mapToPair(new PairFunction<Tuple2<Integer, String>, String, Integer>() {
                            @Override
                            public Tuple2<String, Integer> call(Tuple2<Integer, String> integerStringTuple2) throws Exception {
                                return new Tuple2<String, Integer>(integerStringTuple2._2,integerStringTuple2._1);
                            }
                        });
                        List<Tuple2<String, Integer>> take = finalPRDD.take(3);
                         for(Tuple2<String, Integer> t:take)
                        {
                            System.out.println(t._1+":"+t._2);
                        }
                        return finalPRDD;
                    }
                }
        );
        finalDStream.print();
        jsc.start();
        jsc.awaitTermination();
        jsc.close();
    }


}
