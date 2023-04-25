package org.example;


import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.Dataset;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.ml.evaluation.RegressionEvaluator;
import java.util.Properties;
import static org.apache.spark.sql.functions.*;
public class ForcastingTrial {

    public static void main(String[] args) throws Exception {

        SparkSession spark = SparkSession.builder().master("local[*]").appName("ForcastingTrial").getOrCreate();

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

        //Selecting the desired columnsss
        Dataset<Row> selectedData = teslaRawData.select("Date","Close");

        Dataset<Row> formattedData = selectedData.withColumn("Timestamp", unix_timestamp(col("Date"),"yyyy-MM-dd"));

        Dataset<Row>[] splitData = formattedData.randomSplit(new double[]{0.7,0.3});

        Dataset<Row> trainingData = splitData[0];
        Dataset<Row> testingData = splitData[1];

        LinearRegression lr = new LinearRegression().setMaxIter(10).setRegParam(0.3).setElasticNetParam(0.8);

        LinearRegressionModel lrModel = lr.fit(trainingData);

        Dataset<Row> predictions = lrModel.transform(testingData);
        predictions.show();

        RegressionEvaluator evaluator = new RegressionEvaluator().setLabelCol("Close").setPredictionCol("prediction").setMetricName("rmse");

        double rmse = evaluator.evaluate(predictions);

        System.out.println("RMSE : " + rmse);


    }

}
