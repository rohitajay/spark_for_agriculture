package org.example;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.event.Level;

import java.util.Arrays;
import java.util.logging.Logger;
//import org.apache.spark.sparkContext;



public class CsvToRdd {

    @SuppressWarnings("resource")
    public static void main(String[] args) {

//        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        String path = "/home/administrator/Downloads/spring_pocs/oct_10/spark_for_agriculture/src/main/resources/Data/input.txt";
        JavaRDD<String> initialRdd = sc.textFile(path);
        System.out.println(initialRdd.count());
        initialRdd.flatMap(value -> Arrays.asList(value.split(" ")).iterator());
        initialRdd.foreach(System.out::println);
//        sc.close();
    }

}
