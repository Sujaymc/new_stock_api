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
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "ip-172-31-8-235.eu-west-2.compute.internal:9092",
      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "group.id" -> "group1",
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    // Define the Kafka topic to subscribe to
    val topic = "sujay_stock1"

    // Define the schema for the JSON messages
    val schema = StructType(Seq(
      StructField("symbol", StringType, nullable = true),
      StructField("lastSalePrice", StringType, nullable= true),
      StructField("lastSaleSize", StringType, nullable = true),
      StructField("volume", StringType, nullable = true),
      StructField("askPrice", StringType, nullable = true),
      StructField("bidPrice", StringType, nullable = true),
      StructField("lastUpdated", StringType, nullable = true)
    ))

    // Read the JSON messages from Kafka as a DataFrame
    val df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "ip-172-31-8-235.eu-west-2.compute.internal:9092").option("subscribe", topic).option("startingOffsets", "earliest").load().select(from_json(col("value").cast("string"), schema).as("data")).selectExpr("data.*")

    // Add a formatted timestamp column for partitioning
//    val dfWithTimestamp = df.withColumn("formattedDate", date_format(current_timestamp(), "yyyy-MM-dd_HH-mm-ss"))

    // Write the DataFrame as CSV files to HDFS, partitioned by formatted timestamp
    df.writeStream
      .format("csv")
      .option("checkpointLocation", "/tmp/bigdata_nov_2024/sujay/stock_data/checkpoint")
      .option("path", "/tmp/bigdata_nov_2024/sujay/stock_data/data")
      .start()
      .awaitTermination()
  }
}


//https://site.financialmodelingprep.com/developer/docs/all-realtime-full-prices-quote
//mvn package
//spark-submit --master local --packages "org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.7","com.lihaoyi:requests_2.11:0.7.1" --class spark.stock_to_hdfs target/new_stock_api-1.0-SNAPSHOT.jar