package com.gx;

import com.google.common.base.Optional;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class UpdateStateByKeyStreaming
{
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local[3]").setAppName("wordCount");
        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(5));
        jsc.checkpoint("hdfs://sparkproject1:9000/wordcount_checkpoint");
        JavaReceiverInputDStream<String> lines = jsc.socketTextStream("localhost", 9999);
        JavaDStream<String> words=lines.flatMap(
                new FlatMapFunction<String, String>() {
                    @Override
                    public Iterable<String> call(String s) throws Exception {
                        return Arrays.asList(s.split(" "));
                    }
                }
        );

        JavaPairDStream<String,Integer> pairs=words.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String,Integer>(s,1);
            }
        });

        JavaPairDStream<String,Integer> wordcount=pairs.updateStateByKey(
                new Function2<List<Integer>, Optional<Integer>, Optional<Integer>>() {
                    @Override
                    public Optional<Integer> call(List<Integer> values, Optional<Integer> state) throws Exception {
                        Integer newValue=0;
                        if(state.isPresent())
                        {
                            newValue=state.get();
                        }
                        for(Integer value:values)
                        {
                            newValue+=value;
                        }
                        return Optional.of(newValue);
                    }
                }
        );
        wordcount.print();
        jsc.start();
        jsc.awaitTermination();
        jsc.close();


    }

}
