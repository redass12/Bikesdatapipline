#!/bin/bash
# run mvn package to build the project
mvn package -f pom.xml



# Function to submit Spark jobs via Docker
submit_spark_job() {
    local CLASS_NAME=$1
    echo "$(date '+%Y-%m-%d %H:%M:%S') - Starting Spark job for class: $CLASS_NAME"

    # Define variables
    CLASS="demo.etl.$CLASS_NAME"
    JAR="deploy-spark-standalone-1.0.0-SNAPSHOT.jar"
    SPARK_MASTER_ID=$(docker ps -aqf "name=spark-master-1")
    ARGS=''
    DELTA_PACKAGE="io.delta:delta-core_2.12:1.0.0"

    # Execute Spark submit command
    if docker exec "$SPARK_MASTER_ID" /opt/spark/bin/spark-submit \
        --class $CLASS \
        --master spark://spark-master:7077 \
        --packages $DELTA_PACKAGE \
        --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
        --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
        /opt/spark-apps/$JAR $ARGS
    then
        echo "$(date '+%Y-%m-%d %H:%M:%S') - Completed Spark job for class: $CLASS_NAME successfully."
    else
        echo "$(date '+%Y-%m-%d %H:%M:%S') - Error in Spark job for class: $CLASS_NAME. Check logs for details."
        exit 1
    fi
}

#check_for_txt_files_in_minio() {
#    BUCKET_NAME="rawdata"
#    DIRECTORY_PATH="data/"  # Adjust this to your directory path
#
#    # List all files in the directory and filter for .txt files
#    if mc ls local/$BUCKET_NAME/$DIRECTORY_PATH | grep '\.txt$'; then
#        echo "TXT file found. Proceeding with batch processing..."
#        return 0  # TXT files found
#    else
#        echo "TXT files not found. Exiting..."
#        return 1  # No TXT files found
#    fi
#}

# Sequential execution of Spark jobs

#submit_spark_job "consumeKafkaData"
#
#if check_for_file_in_minio; then
submit_spark_job "BikesLandingToStaging"
submit_spark_job "BikesStagingToAnalytics"
submit_spark_job "BikesAnalyticsToDB"
echo "$(date '+%Y-%m-%d %H:%M:%S') - All Spark jobs completed successfully."
#else
    # This else branch might never be reached due to the loop design,
    # but you could handle unexpected failures here.
    echo "An error occurred."
#fi
echo "$(date '+%Y-%m-%d %H:%M:%S') - All Spark jobs completed successfully."