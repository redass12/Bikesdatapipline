package demo.etl;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;


public class writeDatainDeltaformat {


    public static void main(String[] args) {
        writeDatainDeltaformat app = new writeDatainDeltaformat();
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




        Dataset<Row> orderDF = spark.read()
                .format("delta")
                .load("s3a://analytics/delta-tables/customers");




        orderDF.write().format("delta").mode("overwrite").save("s3a://test/delta-tables/customers");



        spark.stop();
        System.out.println("Delta done!");

    }
}
