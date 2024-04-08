package demo.etl;

import java.util.concurrent.TimeoutException;

import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;

public class consumeKafkaData {


    public static void main(String[] args) {
        consumeKafkaData app = new consumeKafkaData();
        app.start();
    }

    private void start()  {
        // Creates a session on a local master
        // Configuration de Spark
        SparkSession spark = SparkSession.builder()
                .appName("KafkaToMinIO")
                .master("spark://spark-master:7077")
                .getOrCreate();

// Replace these with your MinIO credentials and URL
        String minioAccessKey = "TB9iEvxhbUtRS1wNssg6";
        String minioSecretKey = "tq1stZ3ZwPPZIU59nVcTAOgko9JizEhLPoFun7r3";
        String minioUrl = "http://minio:9000"; // Assuming 'minio' is the hostname in your Docker network

// Configure Spark to use MinIO
        spark.sparkContext().hadoopConfiguration().set("fs.s3a.endpoint", minioUrl);
        spark.sparkContext().hadoopConfiguration().set("fs.s3a.access.key", minioAccessKey);
        spark.sparkContext().hadoopConfiguration().set("fs.s3a.secret.key", minioSecretKey);
        spark.sparkContext().hadoopConfiguration().set("fs.s3a.path.style.access", "true");
        spark.sparkContext().hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
        // Read data from Kafka
        Dataset<Row> df = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "64.227.77.204:9092")
                .option("subscribe", "your_topic")
                .option("startingOffsets", "earliest") // From the beginning of the topic
                .load();

// Assuming your Kafka message value is in string format and you're interested in storing the value
        Dataset<String> values = df.selectExpr("CAST(value AS STRING)").as(Encoders.STRING());

        //Write the streaming data to MinIO
        try {
            values.writeStream()
                    .format("text") // Adjust based on your data format
                    .option("path", "s3a://rawdata/data")
                    .option("checkpointLocation", "s3a://logging/checkpoint")
                    .trigger(Trigger.ProcessingTime("10 seconds"))
                    .option("path", "s3a://rawdata/data")
                    .start()
                    .awaitTermination();
        } catch (StreamingQueryException e) {
            throw new RuntimeException(e);
        } catch (TimeoutException e) {
            throw new RuntimeException(e);
        }



//stop the streaming query
        spark.stop();

    }


    }

