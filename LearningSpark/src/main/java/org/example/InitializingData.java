package org.example;

import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.Dataset;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import static org.apache.spark.sql.functions.*;


public class InitializingData {

    public static void main(String[] args) throws Exception {

        SparkSession spark = SparkSession.builder().master("local[*]").appName("InitalizingData").getOrCreate();

        spark.sparkContext().setLogLevel("Error");

        String url = "jdbc:mysql://localhost:3306/shareprices";
        String user = "root";
        String password = "Newuser@12345";

        Properties connectionProperty =new Properties();
        connectionProperty.put("user", user);
        connectionProperty.put("password", password);

        Dataset<Row> teslaRawData = spark.read().option("header", true).
                option("inferSchema", true).option("multiline", true).csv("data/Tesla.csv");

        Dataset<Row> ferrariRawData = spark.read().option("header", true).
                option("inferSchema", true).option("multiline", true).csv("data/Ferrari.csv");


        System.out.println("Tesla Data");
        teslaRawData.show();
        teslaRawData.write().mode(SaveMode.Overwrite).jdbc(url,"teslaSharePrice",connectionProperty);

        System.out.println("Ferrari Data");
        ferrariRawData.show();
        ferrariRawData.write().mode(SaveMode.Overwrite).jdbc(url,"ferrariSharePrice",connectionProperty);

    }


}
