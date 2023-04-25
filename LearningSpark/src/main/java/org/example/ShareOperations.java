package org.example;

import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.Dataset;

import javax.xml.crypto.Data;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
public class ShareOperations {

    public static void main(String[] args) throws Exception {

        SparkSession spark = SparkSession.builder().master("local[*]").appName("ShareOperations").getOrCreate();

        spark.sparkContext().setLogLevel("Error");

        String url = "jdbc:mysql://localhost:3306/shareprices";
        String user = "root";
        String password = "Newuser@12345";

        Properties connectionProperty =new Properties();
        connectionProperty.put("user", user);
        connectionProperty.put("password", password);

        //Creating Telsa Dataset
        Dataset<Row> teslaRawData = spark.read().option("header", true).option("multiline",true)
                .option("inferSchema", true).jdbc(url,"teslashareprice",connectionProperty);

        //Creating Ferrari Dataset
        Dataset<Row> ferrariRawData = spark.read().option("header",true).option("multiline",true)
                .option("inferSchema",true).jdbc(url,"ferrarishareprice", connectionProperty);

        //Changing the column names of the both the datasets.
        Dataset<Row> teslaChangedColNames = teslaRawData.withColumnRenamed("Open","TeslaOpen")
                .withColumnRenamed("High","TeslaHigh")
                .withColumnRenamed("Low","TeslaLow")
                .withColumnRenamed("Close","TeslaClose")
                .withColumnRenamed("Adj Close","TeslaAdjClose")
                .withColumnRenamed("Volume","TeslaVolume");

        Dataset<Row> ferrariChangedColNames = ferrariRawData.withColumnRenamed("Open","FerraiOpen")
                .withColumnRenamed("High","FerrariHigh")
                .withColumnRenamed("Low","FerrariLow")
                .withColumnRenamed("Close","FerrariClose")
                .withColumnRenamed("Adj Close","FerrariAdjClose")
                .withColumnRenamed("Volume","FerrariVolume");


        //Joining both the tables on the basis of Date to compare share prices on the same Date
        Dataset<Row> teslaFerrariJoinedData = teslaChangedColNames.join(ferrariChangedColNames,"Date");

//        teslaRawData.show();
//        teslaChangedColNames.show();
//        ferrariRawData.show();
//        ferrariChangedColNames.show();

        System.out.println("Joined Data");
        teslaFerrariJoinedData.show();
        teslaFerrariJoinedData.write().jdbc(url,"teslaFerraiJoinedData",connectionProperty);
    }

}
