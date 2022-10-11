package org.example;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;
import org.apache.spark.SparkContext;


public class CsvToDatabase {

    public static void main(String[] args) {

        System.out.println("Hello World");
        String path = "/home/administrator/Downloads/spring_pocs/oct_10/spark_for_agriculture/src/main/resources/Data/Daily_data_of_Evapotranspiration_April2022.csv";

        SparkConf conf = new SparkConf().setAppName("Feedback Analyzer");
        SparkSession spark = SparkSession.builder().
                config(conf)
                .master("local")
                .getOrCreate();

        // Define the Schema of the CSV file
        StructType schema = new StructType(new StructField[] {
                new StructField("State", DataTypes.StringType, false, Metadata.empty()),
                new StructField("District", DataTypes.StringType, false, Metadata.empty()),
                new StructField("Date", DataTypes.DateType, false,Metadata.empty()),
                new StructField("Year",DataTypes.StringType, false,Metadata.empty()),
                new StructField("Month", DataTypes.StringType, false, Metadata.empty()),
                new StructField("Avg_erlvl_at15cm", DataTypes.FloatType, false, Metadata.empty()),
                new StructField("Agency_name",DataTypes.StringType, false, Metadata.empty())

        });

        // Read the CSV to a DataSet
        Dataset<Row> df = spark.read().format("csv")
                .option("header", "true")
                .schema(schema)
                .load(path);
//                .option("mode","PERMISSIVE")
//                .load(args[0]);
        df.show();
     }
}
