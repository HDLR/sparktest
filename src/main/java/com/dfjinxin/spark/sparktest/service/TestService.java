package com.dfjinxin.spark.sparktest.service;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

@Service
public class TestService {
//
//    @Autowired
//    private SparkSession sparkSession;

    public void sql(String file){

//        SQLContext sqlContext = new SQLContext(sc);
//        Dataset filedata = sqlContext.read().json(file);
//        filedata.printSchema();
//        filedata.show();

//        Dataset filedata = sparkSession.read().json(file);
//        filedata.show();
//        sparkSession.stop();

//        filedata.select("name").show();
//        filedata.select(filedata.col("name")).show();
//        filedata.filter(filedata.col("name").equalTo("zhangsan")).show();
//        filedata.select("name", "nums").show();
//        filedata.groupBy("name").count().show();

//        RDD<String> rdd = sparkSession.sparkContext().textFile(file, 0);

//        JavaRDD<Integer> result = rdd.mapPartitions(new FlatMapFunction<Iterator<String>, Integer>() {
//            private static final long serialVersionUID = 1L;
//
//            @Override
//            public Iterator<Integer> call(Iterator<String> iterator) throws Exception{
//                List<Integer> list = new ArrayList<>();
//                while(iterator.hasNext()){
//                    String name = iterator.next();
//                    int score = scoreMap.get(name);
//                    list.add(score);
//                }
//                return list.iterator();
//            }
//        });

        SparkConf conf = new SparkConf().setAppName("MapPartition").setMaster("local[4]");
        JavaSparkContext javaSparkContext = new JavaSparkContext(conf);
        JavaRDD<String> rdd = javaSparkContext.textFile(file);

        JavaPairRDD<String, Integer> nrdd = rdd.flatMap(x -> Arrays.asList(x.split("\t")[1]).iterator()).mapToPair(x -> new Tuple2<>(x, 1));
        nrdd = nrdd.reduceByKey((x, y) -> x + y);
        nrdd.foreach(item -> {
            System.out.println(item._1 + "========" + item._2);
        });

        System.out.println("top---------------------------------------------------------------------------------------------------------------1");

        JavaPairRDD<Integer, String> swaprdd = nrdd.mapToPair(x -> new Tuple2<>(x._2, x._1));
        swaprdd.foreach(item -> {
            System.out.println(item._1 + "========" + item._2);
        });

        System.out.println("top---------------------------------------------------------------------------------------------------------------2");
        JavaPairRDD<Integer, String> swaprddTop = swaprdd.sortByKey(false);
        swaprddTop.foreach(item -> {
            System.out.println(item._1 + "========" + item._2);
        });

        JavaPairRDD<String, Integer> swaprddTop2 = swaprddTop.mapToPair(x -> new Tuple2<>(x._2, x._1));

        System.out.println("top---------------------------------------------------------------------------------------------------------------3");
        swaprddTop2.foreach(item -> {
            System.out.println(item._1 + "========" + item._2);
        });

        List<Tuple2<String, Integer>> l2top = swaprddTop2.take(10);

        System.out.println("top---------------------------------------------------------------------------------------------------------------4");
        for(Tuple2<String, Integer> item : l2top){
            System.out.println(item._1 + "=======" + item._2);
        }

    }
}
