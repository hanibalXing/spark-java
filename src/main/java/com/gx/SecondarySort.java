package com.gx;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.sources.In;
import scala.Tuple2;

/**
 * @author gx
 * @ClassName: SecondarySort
 * @Description: java类作用描述
 * @date 2019/4/8 23:22
 * @Version: 1.0
 * @since
 */
public class SecondarySort {
    public static void main(String[] args) {
        SparkConf conf=new SparkConf().setAppName("spark-gx").setMaster("local");
        JavaSparkContext context=new JavaSparkContext(conf);
        JavaRDD<String> lines=context.textFile("C:\\Users\\pc\\Desktop\\sparkfile\\secondarysort.txt");
        lines.mapToPair(s ->
            new Tuple2<SecondarySortKey,String>(new SecondarySortKey(Integer.parseInt(s.split(" ")[0])
                    , Integer.parseInt(s.split(" ")[1])),s)
        ).sortByKey(false).map(t-> t._2).foreach(s-> System.out.println(s));
    }
}
