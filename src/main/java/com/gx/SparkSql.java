package com.gx;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class SparkSql {
    public static void main(String[] args) {
//reflect();
//otherway();
//join();
        sql();
//test();
    }

    private static  void reflect()
    {
        SparkConf conf=new SparkConf().setAppName("spark-sql").setMaster("local");
        JavaSparkContext context=new JavaSparkContext(conf);
        SQLContext sqlContext=new SQLContext(context);
        JavaRDD<String> lines=context.textFile("C:\\Users\\pc\\Desktop\\sparkfile\\students.txt");
        JavaRDD<Students> students=lines.map(
                new Function<String, Students>() {
                    @Override
                    public Students call(String s) throws Exception {
                        String arr[]=s.split(",");
                        Students student=new Students();
                        student.setId(Integer.parseInt(arr[0]));
                        student.setName(arr[1]);
                        student.setAge(Integer.parseInt(arr[2]));
                        return student;
                    }
                }
        );

        DataFrame studentDF=sqlContext.createDataFrame(students,Students.class);
        studentDF.registerTempTable("students");
        DataFrame youngStudentDF=sqlContext.sql("select * from students where age<=16 order by id");
        JavaRDD<Row> youngStudentRDD=youngStudentDF.javaRDD();
        JavaRDD<Students> studentRDD=youngStudentRDD.map(
                new Function<Row, Students>() {
                    @Override
                    public Students call(Row row) throws Exception {
                        Students s=new Students();
                        s.setId(row.getInt(1));
                        s.setAge(row.getInt(0));
                        s.setName(row.getString(2));
                        return s;
                    }
                }
        );
        List<Students> collect = studentRDD.collect();
        for (Students s:collect)
        {
            System.out.println(s);
        }

    }

    private static void otherway()
    {
        SparkConf conf=new SparkConf().setAppName("spark-sql").setMaster("local");
        JavaSparkContext context=new JavaSparkContext(conf);
        SQLContext sqlContext=new SQLContext(context);
        JavaRDD<String> lines=context.textFile("C:\\Users\\pc\\Desktop\\sparkfile\\students.txt");
        JavaRDD<Row> studentsRow=lines.map(
                new Function<String, Row>() {
                    @Override
                    public Row call(String s) throws Exception {
                        String arr[]=s.split(",");
                        return RowFactory.create(Integer.valueOf(arr[0]),arr[1],Integer.valueOf(arr[2]));
                    }
                }
        );

        List<StructField> fields=new ArrayList<StructField>();
        fields.add(DataTypes.createStructField("id",DataTypes.IntegerType,true));
        fields.add(DataTypes.createStructField("name",DataTypes.StringType,true));
        fields.add(DataTypes.createStructField("age",DataTypes.IntegerType,true));

        StructType structType=DataTypes.createStructType(fields);
        DataFrame studentDF=sqlContext.createDataFrame(studentsRow,structType);
        studentDF.registerTempTable("students");
        DataFrame youngStudentDF=sqlContext.sql("select * from students where age<=16 order by age");
       List<Row> rows=youngStudentDF.javaRDD().collect();
       for (Row row:rows)
       {
           System.out.println(row);
       }
    }


    private static void join()
    {
        SparkConf conf=new SparkConf().setAppName("spark-sql").setMaster("local");
        JavaSparkContext context=new JavaSparkContext(conf);
        SQLContext sqlContext=new SQLContext(context);
        JavaRDD<String> lines=context.textFile("C:\\Users\\pc\\Desktop\\sparkfile\\students.txt");
        JavaRDD<Row> studentsRow=lines.map(
                new Function<String, Row>() {
                    @Override
                    public Row call(String s) throws Exception {
                        String arr[]=s.split(",");
                        return RowFactory.create(Integer.valueOf(arr[0]),arr[1],Integer.valueOf(arr[2]));
                    }
                }
        );

        List<StructField> fields=new ArrayList<StructField>();
        fields.add(DataTypes.createStructField("id",DataTypes.IntegerType,true));
        fields.add(DataTypes.createStructField("name",DataTypes.StringType,true));
        fields.add(DataTypes.createStructField("age",DataTypes.IntegerType,true));

        StructType structType=DataTypes.createStructType(fields);
        DataFrame studentDF=sqlContext.createDataFrame(studentsRow,structType);
        studentDF.registerTempTable("students");

        JavaRDD<String> scorelines=context.textFile("C:\\Users\\pc\\Desktop\\sparkfile\\score.txt");
        JavaRDD<Row> scoreRow=scorelines.map(
                new Function<String, Row>() {
                    @Override
                    public Row call(String s) throws Exception {
                        String arr[]=s.split(",");
                        return RowFactory.create(Integer.valueOf(arr[0]),Integer.valueOf(arr[1]));
                    }
                }
        );
        List<StructField> scorefields=new ArrayList<StructField>();
        scorefields.add(DataTypes.createStructField("id",DataTypes.IntegerType,true));
        scorefields.add(DataTypes.createStructField("score",DataTypes.IntegerType,true));
        StructType scoreStructType=DataTypes.createStructType(scorefields);
        DataFrame scoreDF=sqlContext.createDataFrame(scoreRow,scoreStructType);
        scoreDF.registerTempTable("score");

        DataFrame youngStudentDF=sqlContext.sql("select st.name ,sc.score from students  st,score sc where st.id=sc.id ");
        List<Row> rows=youngStudentDF.javaRDD().collect();
        for (Row row:rows)
        {
            System.out.println(row);
        }


    }


    private static void sql()
    {
        SparkConf conf=new SparkConf().setAppName("spark-sql").setMaster("local");
        JavaSparkContext context=new JavaSparkContext(conf);
        SQLContext sqlContext=new SQLContext(context);
        Map<String,String> option=new HashMap<String,String>();
        option.put("url","jdbc:mysql://localhost:3306/fcbb_b2b2c_dev?useUnicode=true&characterEncoding=UTF-8&zeroDateTimeBehavior=convertToNull");
        option.put("user","root");
        option.put("password","gx1984");
        option.put("dbtable","iskyshop_search_log");
        DataFrame searchlog=sqlContext.read().format("jdbc").options(option).load();
        JavaPairRDD<String,Integer> pair= searchlog.javaRDD().mapToPair(new PairFunction<Row, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Row row) throws Exception {
                return new Tuple2<String,Integer>(String.valueOf(row.get(3)),1);
            }
        });
        JavaPairRDD<String,Integer> countpair=pair.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer+integer2;
            }
        });
        AtomicInteger i=new AtomicInteger(0);
        countpair.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> tuple) throws Exception {
                System.out.println(tuple._1+"=========="+tuple._2);

            }
        });




    }

    private static void test()
    {
        AtomicInteger i=new AtomicInteger(0);
        for(int j=1;j<10;j++)
        {
            i.getAndAdd(j);
        }
        System.out.println(i.get());
    }

}
