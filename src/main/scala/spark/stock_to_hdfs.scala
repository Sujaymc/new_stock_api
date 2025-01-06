package spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object stock_to_hdfs {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("KafkaToHDFS")
      .master("local[*]")
      .getOrCreate()

    // Define the Kafka parameters
    val kafkaBootstrapServers = "ip-172-31-8-235.eu-west-2.compute.internal:9092"
    val topic = "sujay_stock1"

    // Define the schema for the JSON messages
    val schema = StructType(Seq(
      StructField("symbol", StringType, true),
      StructField("lastSalePrice", DoubleType, true),
      StructField("lastSaleSize", IntegerType, true),
      StructField("volume", IntegerType, true),
      StructField("askPrice", DoubleType, true),
      StructField("bidPrice", DoubleType, true),
      StructField("lastUpdated", LongType, true)
    ))

    // Read the JSON messages from Kafka as a DataFrame
    val df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "ip-172-31-8-235.eu-west-2.compute.internal:9092").option("subscribe", topic).option("startingOffsets", "earliest").load().select(from_json(col("value").cast("string"), schema).as("data")).selectExpr("data.*")

    // Add a formatted timestamp column for partitioning
    val dfWithTimestamp = df.withColumn("formattedDate", date_format(current_timestamp(), "yyyy-MM-dd_HH-mm-ss"))

    // Write the DataFrame as CSV files to HDFS, partitioned by formatted timestamp
    dfWithTimestamp.writeStream
      .format("csv")
      .option("checkpointLocation", "/tmp/bigdata_nov_2024/sujay/checkpoint_stock_list")
      .option("path", "/tmp/bigdata_nov_2024/sujay/data_stock_list")
      .partitionBy("formattedDate")
      .start()
      .awaitTermination()
  }
}
