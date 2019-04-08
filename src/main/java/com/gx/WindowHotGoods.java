package com.gx;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

//每10秒统计最近60秒的每个分类下的热门商品
public class WindowHotGoods
{
    public static void main(String[] args) {
        SparkConf conf=new SparkConf().setMaster("local[2]").setAppName("hotGoodsCount");
        JavaStreamingContext jsc=new JavaStreamingContext(conf, Durations.seconds(2));
        JavaReceiverInputDStream<String> goodsLogDStream=jsc.socketTextStream("192.168.1.2",9999);
        //gx iphone phone
        //gx zara clothes
        //gx hm clothes
        JavaPairDStream<String,Integer> mapDStream=goodsLogDStream.mapToPair(
                new PairFunction<String, String, Integer>() {
                    @Override
                    public Tuple2<String, Integer> call(String s) throws Exception {
                        String arr[]=s.split(" ");
                        //组织成类似clothes_zara 1
                        return new Tuple2<String,Integer>(arr[2]+"_"+arr[1],1);
                    }
                }
        );

        JavaPairDStream<String,Integer> countsStream=mapDStream.reduceByKeyAndWindow(
                new Function2<Integer, Integer, Integer>() {
                    @Override
                    public Integer call(Integer integer, Integer integer2) throws Exception {
                        return integer+integer2;
                    }
                },Durations.seconds(60),Durations.seconds(10)
        );
        countsStream.foreachRDD(
                new Function<JavaPairRDD<String, Integer>, Void>() {
                    @Override
                    public Void call(JavaPairRDD<String, Integer> pairRDD) throws Exception {
                        JavaRDD<Row> rowRDD=pairRDD.map(new Function<Tuple2<String, Integer>, Row>() {
                            @Override
                            public Row call(Tuple2<String, Integer> tuple) throws Exception {
                               String category=tuple._1.split("_")[0];
                               String product=tuple._1.split("_")[1];

                                return RowFactory.create(category,product,tuple._2);
                            }
                        });
                        List<StructField> fields=new ArrayList<StructField>();
                        fields.add(DataTypes.createStructField("category", DataTypes.StringType,true));
                        fields.add(DataTypes.createStructField("product", DataTypes.StringType,true));
                        fields.add(DataTypes.createStructField("click_count", DataTypes.IntegerType,true));
                        StructType structType=DataTypes.createStructType(fields);
                        HiveContext sqlContext=new HiveContext(rowRDD.context());
                        DataFrame df=sqlContext.createDataFrame(rowRDD,structType);
                        df.registerTempTable("goods_product");
                        DataFrame top3DF=sqlContext.sql(
                                "select category,product,click_count "
                                        +"from ("
                                        +"select category,product,click_count,row_number()"
                                        +" over (partition by category order by click_count desc) rank from goods_product" +
                                        ") temp " +
                                        "where rank<=3");
                        top3DF.show();
                        return null;
                    }
                }
        );
        jsc.start();
        jsc.awaitTermination();
        jsc.close();
    }





}
