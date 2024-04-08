package demo.etl;


import org.apache.spark.mllib.fpm.PrefixSpan;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.functions.*;


public class BikesLandingToStaging {
    public static void main(String[] args) {
        BikesLandingToStaging app = new BikesLandingToStaging();
        app.start();
    }

    private void start() {

        SparkSession spark = SparkSession.builder()
                .appName("Delta Lake with MinIO Example")
                .master("spark://spark-master:7077")
                // Définir les configurations pour accéder à MinIO
                .config("spark.hadoop.fs.s3a.endpoint", " http://minio:9000")
                .config("spark.hadoop.fs.s3a.access.key", "TB9iEvxhbUtRS1wNssg6")
                .config("spark.hadoop.fs.s3a.secret.key", "tq1stZ3ZwPPZIU59nVcTAOgko9JizEhLPoFun7r3")
                .config("spark.hadoop.fs.s3a.path.style.access", true)
                .config("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore")
                .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
                .getOrCreate();



        Dataset<Row> bikesDF = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .option("delimiter", ";")
                .csv("s3a://landing/bikes.csv");

        // Load the original bikeshops CSV table
        Dataset<Row> bikeshopsDF = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .option("delimiter", ";")
                .csv("s3a://landing/bikeshops.csv");

        Dataset<Row> customerDF = spark.read()
                .option("header", "true") // Assumes the CSV has a header
                .option("inferSchema", "true")// Spark will infer data types
                .option("delimiter", ",")
                .csv("s3a://landing/customers.csv");

        Dataset<Row> orderDF = spark.read()
                .option("header" , "true")
                .option("inferSchema", "true")
                .option("delimiter", ";")
                .csv("s3a://landing/orders.csv");


        //-----------------------------------------------------------------------------------

        // Apply transformations to the bikes DataFrame
        Dataset<Row> cleanedBikesDF = bikesDF
                .withColumnRenamed("bike.id", "bike_id")
                .na().drop()
                .dropDuplicates("bike_id");

        // Apply transformations to the bikeshops DataFrame


        Dataset<Row> cleanedBikeshopsDF = bikeshopsDF
                .withColumnRenamed("bikeshop.id", "bikeshop_id")
                .withColumnRenamed("bikeshop.name", "bikeshop_name")
                .withColumnRenamed("bikeshop.city", "city")
                .withColumnRenamed("bikeshop.state", "state")
                .withColumn("bikeshop_name", trim(col("bikeshop_name")))
                .withColumn("city", lower(trim(col("city"))))
                .withColumn("state", upper(trim(col("state"))))
                .withColumn("latitude", regexp_replace(col("latitude"), ",", ".").cast("double"))
                .withColumn("longitude", regexp_replace(col("longitude"), ",", ".").cast("double"))
                .withColumn("full_location", concat_ws(", ", col("city"), col("state")))
                .withColumn("latitude",
                        when(col("latitude").geq(-90).and(col("latitude").leq(90)), col("latitude"))
                                .otherwise(null))
                .withColumn("longitude",
                        when(col("longitude").geq(-180).and(col("longitude").leq(180)), col("longitude"))
                                .otherwise(null))
                .na().drop()
                .dropDuplicates("bikeshop_id");


         // Apply transformations to the Customer DataFrame

        Dataset<Row> customerTrimspaceDF = null;

        for (String column : customerDF.columns()) {
            customerDF= customerDF.withColumnRenamed(column, column.trim());
            customerDF = customerDF.withColumn(column.trim(), trim(col(column.trim())));
        }



        Dataset<Row> cleanedCustomerDF = customerDF
                .withColumn("EmailAddress", trim(col("EmailAddress")))
                .withColumn("FullName", concat_ws(" ", col("FirstName"), col("LastName")))
                .withColumn("BirthDate", to_date(col("BirthDate"), "dd/MM/yyyy"))
                .withColumn("AnnualIncome", col("AnnualIncome").cast(DataTypes.IntegerType))
                .withColumn("TotalChildren", col("TotalChildren").cast(DataTypes.IntegerType))
                .filter(col("EmailAddress").rlike("^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,6}$"))
                .filter(col("TotalChildren").geq(0))
                .withColumn("Age", year(current_date()).minus(year(col("BirthDate"))))
                .withColumn("HighIncomeFlag", when(col("AnnualIncome").gt(100000), "Yes").otherwise("No"))
                .withColumn("MaritalStatus", upper(col("MaritalStatus")))
                .withColumn("Gender", upper(col("Gender")))
                .drop("Prefix" , "FirsName","LastName")
                .na().drop()
                .dropDuplicates("CustomerKey");

        Dataset<Row>CleanedAndOrdredCustomerDF = cleanedCustomerDF.select(
                col("CustomerKey"),
                col("FullName"),
                col("BirthDate"),
                col("Age"),
                col("MaritalStatus"),
                col("Gender"),
                col("EmailAddress"),
                col("AnnualIncome"),
                col("TotalChildren"),
                col("EducationLevel"),
                col("Occupation"),
                col("HomeOwner"),
                col("HighIncomeFlag")
        );

        // Apply transformations to the Orders DataFrame

        Dataset<Row> CleanedOrderDF = orderDF
                .withColumnRenamed("order.id", "orderId")
                .withColumnRenamed("order.line", "BikeShopId")
                .withColumnRenamed("order.date", "orderDate")
                .withColumnRenamed("customer.id", "customerId")
                .withColumnRenamed("product.id", "bikeId")
                .withColumnRenamed("quantity", "quantityOrdered");

        // Correct the date format
        CleanedOrderDF = CleanedOrderDF
                .withColumn("orderDate", to_date(col("orderDate"), "M/d/yyyy"));

        // Drop duplicates and rows with null values in critical columns
        CleanedOrderDF = CleanedOrderDF
                .na().drop()
                .dropDuplicates()
                .drop("_c0");


// testing :

//        // Uniqueness check: Ensuring orderId is unique
//        Dataset<Row> distinctBikesDF = cleanedBikesDF.distinct();
//        if (distinctBikesDF.count() != cleanedBikesDF.count()) {
//            System.out.println("Warning: Duplicate order IDs found.");
//        }
//
//
//        Dataset<Row> distinctBikeshopsDF = cleanedBikeshopsDF.distinct();
//        if (distinctBikeshopsDF.count() != cleanedBikeshopsDF.count()) {
//            System.out.println("Warning: Duplicate order IDs found.");
//        }
//
//
//        Dataset<Row> distinctCustomersDF = CleanedAndOrdredCustomerDF.distinct();
//        if (distinctCustomersDF.count() != CleanedAndOrdredCustomerDF.count()) {
//            System.out.println("Warning: Duplicate order IDs found.");
//        }
//
//        Dataset<Row> distinctOrdersDF = CleanedOrderDF.distinct();
//        if (distinctOrdersDF.count() != CleanedOrderDF.count()) {
//            System.out.println("Warning: Duplicate order IDs found.");
//        }
//        // Referential integrity check: Ensuring every orderId in ordersDF has a matching customerId in customersDF
//        Dataset<Row> joinedBikeOrdersDF = cleanedBikesDF.join(CleanedOrderDF, cleanedBikesDF.col("bike_id").equalTo(CleanedOrderDF.col("bikeId")), "left_anti");
//        if (joinedBikeOrdersDF.count() > 0) {
//            System.out.println("Warning: Orders with non-existing product.");
//        }
//
//        Dataset<Row> JoinedBikeshopOrdersDF = cleanedBikeshopsDF.join(CleanedOrderDF, CleanedOrderDF.col("BikeShopId").equalTo(cleanedBikeshopsDF.col("bikeshop_id")), "left_anti");
//        if (JoinedBikeshopOrdersDF.count() > 0) {
//            System.out.println("Warning: Orders with non-existing Bike shop.");
//        }
//
//
//        Dataset<Row> joinedDF = CleanedOrderDF
//                .join(cleanedBikeshopsDF, CleanedOrderDF.col("bikeshopId").equalTo(cleanedBikeshopsDF.col("bikeshop_id")), "left_semi");
//
//        // Show the result
//        joinedDF.show();

        // Check if there are any rows in the joined dataset
//        if (joinedDF.count() > 0) {
//            System.out.println("There are orders linked to bike shops.");
//        } else {
//            System.out.println("No orders linked to bike shops.");
//        }
//        // Data type and format validations: Checking date format in ordersDF
//        Dataset<Row> invalidDatesDF = ordersDF.filter(not(col("orderDate").rlike("\\d{4}-\\d{2}-\\d{2}")));
//        if (invalidDatesDF.count() > 0) {
//            System.out.println("Warning: Orders with invalid date formats found.");
//        }


        //write data to staging :

        cleanedBikesDF.write()
                .format("delta")
                .save("s3a://staging/delta-tables/Bike");

        cleanedBikeshopsDF.write()
                .format("delta")
                .save("s3a://staging/delta-tables/Bikeshop");

        CleanedAndOrdredCustomerDF.write()
                .format("delta")
                .save("s3a://staging/delta-tables/Customer");

        CleanedOrderDF.write()
                .format("delta")
                .save("s3a://staging/delta-tables/Order");
//test of null and empty values in the Order dataframe:
//        for (String column : CleanedOrderDF.columns()) {
//            long nullCount = CleanedOrderDF.filter(col(column).isNull().or(col(column).equalTo(""))).count();
//            System.out.println("Number of null values in column " + column + ": " + nullCount);
//        }




        // Stop the Spark session
        spark.stop();




    }
}
