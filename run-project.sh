mvn package -f pom.xml
export CLASS=demo.etl.writeDatainDeltaformat
export JAR=deploy-spark-standalone-1.0.0-SNAPSHOT.jar
SPARK_MASTER_ID="$(docker ps -aqf "name=spark-master-1")"
export SPARK_MASTER_ID
export ARGS=''

# Specify the Delta Lake package version compatible with your Spark version
DELTA_PACKAGE="io.delta:delta-core_2.12:1.0.0"


docker exec -it "$SPARK_MASTER_ID" /opt/spark/bin/spark-submit \
    --class $CLASS \
    --master spark://spark-master:7077 \
    --packages $DELTA_PACKAGE \
    --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
    --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
    /opt/spark-apps/$JAR $ARGS
