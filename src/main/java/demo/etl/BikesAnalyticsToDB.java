package demo.etl;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.util.Properties;

import static org.apache.spark.sql.functions.*;

public class BikesAnalyticsToDB {

    public static void main(String[] args) {
        BikesAnalyticsToDB app = new BikesAnalyticsToDB();
        app.start();
    }

    private void start() {
        // Creates a session on a local master

        SparkSession spark = SparkSession.builder()
                .appName("Delta Lake with MinIO Example")
                .master("spark://spark-master:7077")
                // Définir les configurations pour accéder à MinIO
                .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
                .config("spark.hadoop.fs.s3a.access.key", "TB9iEvxhbUtRS1wNssg6")
                .config("spark.hadoop.fs.s3a.secret.key", "tq1stZ3ZwPPZIU59nVcTAOgko9JizEhLPoFun7r3")
                .config("spark.hadoop.fs.s3a.path.style.access", true)
                .config("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore")
                .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
                .getOrCreate();


        Dataset<Row> bikesDF = spark.read().format("delta").load("s3a://analytics/delta-tables/bikes");
        Dataset<Row> bikeshopsDF = spark.read().format("delta").load("s3a://analytics/delta-tables/bikeshops");
        Dataset<Row> customersDF = spark.read().format("delta").load("s3a://analytics/delta-tables/customers");
        Dataset<Row> ordersDF = spark.read().format("delta").load("s3a://analytics/delta-tables/orders");



        // Aggregate sales data
        Dataset<Row> salesDF = ordersDF.join(bikesDF, ordersDF.col("bikeId").equalTo(bikesDF.col("bike_id")))
                .join(bikeshopsDF, ordersDF.col("BikeShopId").equalTo(bikeshopsDF.col("bikeshop_id")))
                .groupBy("BikeShopId", "city", "state", "bike_id", "model", "category1", "category2", "orderDate")
                .agg(
                        sum("quantityOrdered").as("total_quantity"),
                        sum(expr("quantityOrdered * price")).as("total_sales"),
                        countDistinct("orderId").as("total_orders")
                )
                .withColumn("order_year", year(col("orderDate")))
                .withColumn("order_month", month(col("orderDate")));


        // Customer Lifetime Value and New Customers
        Dataset<Row> customerAggregatedDF = ordersDF
                .join(customersDF, ordersDF.col("customerId").equalTo(customersDF.col("CustomerKey")), "left_outer")
                .join(bikesDF, ordersDF.col("bikeId").equalTo(bikesDF.col("bike_id")), "left_outer")
                .join(bikeshopsDF, ordersDF.col("BikeShopId").equalTo(bikeshopsDF.col("bikeshop_id")), "left_outer")
                .groupBy(customersDF.col("CustomerKey"), customersDF.col("FullName"), customersDF.col("BirthDate"), bikeshopsDF.col("city"), bikeshopsDF.col("state"),customersDF.col("Age"),customersDF.col("Gender"))
                .agg(
                        sum(expr("quantityOrdered * price")).as("lifetime_value"),
                        min(ordersDF.col("orderDate")).as("first_order_date"),
                        countDistinct(ordersDF.col("orderId")).as("total_orders")
                );





        customerAggregatedDF.show();

// Additional customer segmentation can be performed here based on the columns available








// Save to PostgreSQL in the cloud
        String dbConnectionUrl = "jdbc:postgresql://64.227.77.204:5432/bikeDB";

// Properties to connect to the database
        Properties prop = new Properties();
        prop.setProperty("driver", "org.postgresql.Driver");
        prop.setProperty("user", "postgres");
        prop.setProperty("password", "postgres");

// Additional properties might be required depending on your cloud provider
// For example, SSL connection properties for secure connections
        prop.setProperty("sslfactory", "org.postgresql.ssl.NonValidatingFactory"); // Use this for a cloud provider that doesn't require client-side SSL certificate validation

        salesDF.write()
                .mode(SaveMode.Overwrite)
                .jdbc(dbConnectionUrl, "Sales", prop);

        customerAggregatedDF.write()
                .mode(SaveMode.Overwrite)
                .jdbc(dbConnectionUrl, "Customers", prop);



    }

    }
