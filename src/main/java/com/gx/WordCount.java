package com.gx;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Hello world!
 *
 */
public class WordCount
{
    public static void main( String[] args )
    {

        basic();
        //sparksql();




    }

    //传统方式wordcount排序
    private static void basic()
    {
        SparkConf conf=new SparkConf().setAppName("spark-gx").setMaster("local");
        JavaSparkContext context=new JavaSparkContext(conf);
        JavaRDD<String> lines=context.textFile("C:\\Users\\pc\\Desktop\\sparkfile\\wordcount.txt");
        JavaRDD<String> words= lines.flatMap(new FlatMapFunction<String, String>()
        {

            @Override
            public Iterable<String> call(String line) {
                return Arrays.asList(line.split(" "));
            }
        });

        JavaPairRDD<String,Integer> pairs=words.mapToPair(
                new PairFunction<String, String, Integer>() {
                    @Override
                    public Tuple2<String, Integer> call(String s) throws Exception {
                        return new Tuple2<String, Integer>(s,1);
                    }
                }
        );
        JavaPairRDD<String,Integer> wordCount=pairs.reduceByKey(
                new Function2<Integer, Integer, Integer>() {
                    @Override
                    public Integer call(Integer integer, Integer integer2) throws Exception {
                        return integer+integer2;
                    }
                }
        );

        JavaPairRDD<Integer,String> counttokey=wordCount.mapToPair(
                new PairFunction<Tuple2<String, Integer>, Integer, String>() {
                    @Override
                    public Tuple2<Integer, String> call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                        return new Tuple2<Integer,String>(stringIntegerTuple2._2,stringIntegerTuple2._1);
                    }
                }
        );
        JavaPairRDD<Integer,String>  sort=counttokey.sortByKey(false);

        sort.foreach(
                new VoidFunction<Tuple2<Integer, String>>() {
                    @Override
                    public void call(Tuple2<Integer, String> wordCount) throws Exception {
                        System.out.println(wordCount._2+"appeared ============= "+wordCount._1+"       times         ");
                    }
                }
        );
        context.close();
       /* wordCount.foreach(
                new VoidFunction<Tuple2<String, Integer>>() {
                    @Override
                    public void call(Tuple2<String, Integer> wordCount) throws Exception {
                        System.out.println(wordCount._1+"appeared ============= "+wordCount._2+"       times         ");
                    }
                }
        );*/
    }

    private static void sparksql()
    {
        SparkConf conf=new SparkConf().setAppName("spark-sql").setMaster("local");
        JavaSparkContext context=new JavaSparkContext(conf);
        SQLContext sqlContext=new SQLContext(context);
        JavaRDD<String> lines=context.textFile("C:\\Users\\pc\\Desktop\\sparkfile\\wordcount.txt");
        JavaRDD<String> words= lines.flatMap(new FlatMapFunction<String, String>()
        {

            @Override
            public Iterable<String> call(String line) {
                return Arrays.asList(line.split(" "));
            }
        });

        JavaPairRDD<String,Integer> pairs=words.mapToPair(
                new PairFunction<String, String, Integer>() {
                    @Override
                    public Tuple2<String, Integer> call(String s) throws Exception {
                        return new Tuple2<String, Integer>(s,1);
                    }
                }
        );
        JavaPairRDD<String,Integer> wordCount=pairs.reduceByKey(
                new Function2<Integer, Integer, Integer>() {
                    @Override
                    public Integer call(Integer integer, Integer integer2) throws Exception {
                        return integer+integer2;
                    }
                }
        );

        JavaRDD<Row> wordRow=wordCount.map(
                new Function<Tuple2<String, Integer>, Row>() {
                    @Override
                    public Row call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                        return RowFactory.create(stringIntegerTuple2._1,stringIntegerTuple2._2);
                    }
                }
        );

        List<StructField> fields=new ArrayList<StructField>();

        fields.add(DataTypes.createStructField("word",DataTypes.StringType,true));
        fields.add(DataTypes.createStructField("count",DataTypes.IntegerType,true));

        StructType structType=DataTypes.createStructType(fields);
        DataFrame wordDF=sqlContext.createDataFrame(wordRow,structType);
        wordDF.registerTempTable("words");
        DataFrame wordOrderDF=sqlContext.sql("select * from words  order by count desc");
        List<Row> rows=wordOrderDF.javaRDD().collect();
        for (Row row:rows)
        {
            System.out.println(row);
        }

    }


}
