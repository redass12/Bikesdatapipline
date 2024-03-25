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
                .config("spark.hadoop.fs.s3a.endpoint", "http://172.20.0.4:9000")
                .config("spark.hadoop.fs.s3a.access.key", "iz3mZTYYtkHKSJj93Jdk")
                .config("spark.hadoop.fs.s3a.secret.key", "7n7ydGrkFnGGJDd5lSJG4MAECL8bFakKHVddr1ew")
                .config("spark.hadoop.fs.s3a.path.style.access", true)
                .config("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore")
                .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
                .getOrCreate();


        Dataset<Row> bikesDF = spark.read().format("delta").load("s3a://analytics/delta-tables/bikes");
        Dataset<Row> bikeshopsDF = spark.read().format("delta").load("s3a://analytics/delta-tables/bikeshops");
        Dataset<Row> customersDF = spark.read().format("delta").load("s3a://analytics/delta-tables/customers");
        Dataset<Row> ordersDF = spark.read().format("delta").load("s3a://analytics/delta-tables/orders");



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







        // Save to postgresql
        String dbConnectionUrl = "jdbc:postgresql://psql-database:5432/bikesdb";


        // Properties to connect to the database, the JDBC driver is part of our
        // pom.xml
        Properties prop = new Properties();

        prop.setProperty("driver", "org.postgresql.Driver");
        prop.setProperty("user", "postgres");
        prop.setProperty("password", "postgres");


        salesDF.write()
                .mode(SaveMode.Overwrite)
                .jdbc(dbConnectionUrl, "Sales", prop);


    }

    }
