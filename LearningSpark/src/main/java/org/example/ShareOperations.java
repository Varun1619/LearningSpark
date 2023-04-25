package org.example;

import org.apache.spark.internal.config.R;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.Dataset;

import javax.xml.crypto.Data;
import java.awt.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import org.apache.spark.sql.expressions.*;
import org.apache.spark.sql.expressions.Window;

import static org.apache.spark.sql.functions.*;
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

        Dataset<Row> ferrariChangedColNames = ferrariRawData.withColumnRenamed("Open","FerrariOpen")
                .withColumnRenamed("High","FerrariHigh")
                .withColumnRenamed("Low","FerrariLow")
                .withColumnRenamed("Close","FerrariClose")
                .withColumnRenamed("Adj Close","FerrariAdjClose")
                .withColumnRenamed("Volume","FerrariVolume");


        //Joining both the tables on the basis of Date to compare share prices on the same Date
        Dataset<Row> teslaFerrariJoinedData = teslaChangedColNames.join(ferrariChangedColNames,"Date");


        //Doing Basic statistics operations....
        //Mean of Close, High, Open, Close

        Dataset<Row> meanSharePrices = teslaFerrariJoinedData.groupBy()
                .agg(mean("TeslaOpen"),mean("TeslaHigh"),mean("TeslaLow"),mean("TeslaClose"),
                        mean("FerrariOpen"), mean("FerrariHigh"), mean("FerrariLow"),mean("FerrariCLose"));

        meanSharePrices.show();


        //testing the correlation between the closing price of Tesla
        double correlationTeslaCloseVolume = teslaChangedColNames.stat().corr("TeslaClose","TeslaVolume");

        //testing the correlation between the closing price of Ferrari
        double correlationFerrariCloseVolume = ferrariChangedColNames.stat().corr("FerrariClose","FerrariVolume");

        //testing the correlation between the closing price of Tesla and Ferrari
        double correlationFerrariTesalClose = teslaFerrariJoinedData.stat().corr("FerrariClose","TeslaClose");

        System.out.println(correlationTeslaCloseVolume);
        System.out.println(correlationFerrariCloseVolume);
        System.out.println(correlationFerrariTesalClose);

        //Analyzing the volume of the two companies.
        Dataset<Row> volumeCount = teslaFerrariJoinedData.groupBy()
                .agg(sum("TeslaVolume"),sum("FerrariVolume"));
        volumeCount.show();

        //Window Technique to find the percentage change in volume


        teslaFerrariJoinedData.write().mode(SaveMode.Overwrite).jdbc(url,"teslaFerraiJoinedData",connectionProperty);
    }

}
