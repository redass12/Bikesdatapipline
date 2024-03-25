package demo.etl;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.Window;

import static org.apache.spark.sql.functions.*;

public class BikesStagingToAnalytics {

    public static void main(String[] args) {
        BikesStagingToAnalytics app = new BikesStagingToAnalytics();
        app.start();
    }

    private void start() {

        SparkSession spark = SparkSession.builder()
                .appName("Delta Lake with MinIO Example")
                .master("spark://spark-master:7077")
                // Définir les configurations pour accéder à MinIO
                .config("spark.hadoop.fs.s3a.endpoint", "http://172.20.0.4:9000")
                .config("spark.hadoop.fs.s3a.access.key", "iz3mZTYYtkHKSJj93Jdk")
                .config("spark.hadoop.fs.s3a.secret.key", "7n7ydGrkFnGGJDd5lSJG4MAECL8bFakKHVddr1ew")
                .config("spark.hadoop.fs.s3a.path.style.access", true)
                .config("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore")
                .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
                .getOrCreate();

        // Load the staging data into DataFrames
        Dataset<Row> bikesDF = spark.read().format("delta").load("s3a://staging/delta-tables/Bike");
        Dataset<Row> bikeshopsDF = spark.read().format("delta").load("s3a://staging/delta-tables/Bikeshop");
        Dataset<Row> customersDF = spark.read().format("delta").load("s3a://staging/delta-tables/Customer");
        Dataset<Row> ordersDF = spark.read().format("delta").load("s3a://staging/delta-tables/Oerder");



        // Prepare the fact table by joining ordersDF with the dimension tables to add foreign keys
        Dataset<Row> ordersFactDF = ordersDF
                .join(bikesDF, ordersDF.col("bikeId").equalTo(bikesDF.col("bike_id")), "left_outer")
                .join(bikeshopsDF, ordersDF.col("BikeShopId").equalTo(bikeshopsDF.col("bikeshop_id")), "left_outer")
                .join(customersDF, ordersDF.col("customerId").equalTo(customersDF.col("CustomerKey")), "left_outer")
                .select(ordersDF.col("orderId"), ordersDF.col("orderDate"), ordersDF.col("bikeId"), ordersDF.col("BikeShopId"), ordersDF.col("customerId"), ordersDF.col("quantityOrdered"));



        ordersFactDF.write().format("delta").mode("overwrite").save("s3a://analytics/delta-tables/orders");
        bikesDF.write().format("delta").mode("overwrite").save("s3a://analytics/delta-tables/bikes");
        bikeshopsDF.write().format("delta").mode("overwrite").save("s3a://analytics/delta-tables/bikeshops");
        customersDF.write().format("delta").mode("overwrite").save("s3a://analytics/delta-tables/customers");

    }


}
