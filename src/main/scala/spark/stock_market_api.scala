package spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object stock_read_api {

  def main(args: Array[String]): Unit = {
    // Start Spark session
    val spark = SparkSession.builder()
      .appName("Stock API Reader")
      .master("local[*]")
      .getOrCreate()

    while (true) {
      import spark.implicits._

      // API details
      val apiUrl = "https://financialmodelingprep.com/api/v3/stock/full/real-time-price?apikey=gALCXhaqkWuEhIhyrQTqqsxwdoVtKD7I"
      val response = get(apiUrl)
      val jsonResponse = response.text()
      // Parse JSON response
      val dfFromText = spark.read.json(Seq(jsonResponse).toDS)

      // Show data for debugging
      val stockDF = dfFromText.select(
        $"symbol",
        $"lastSalePrice",
        $"lastSaleSize",
        $"volume",
        $"askPrice",
        $"bidPrice",
        $"lastUpdated"
      )

      stockDF.show(5, truncate = false)

      // Kafka server and topic name assignment
      //      val kafkaServer: String = "ip-172-31-8-235.eu-west-2.compute.internal:9092"
      //      val topicSampleName: String = "sujay_topic1" // Your Kafka topic name

      // Write data to Kafka
      //      stockDF.selectExpr("CAST(symbol AS STRING) AS key", "to_json(struct(*)) AS value")
      //        .write
      //        .format("kafka")
      //        .option("kafka.bootstrap.servers", kafkaServer)
      //        .option("topic", topicSampleName)
      //        .save()

      println("Message is loaded to Kafka topic")
      Thread.sleep(10000) // Wait for 10 seconds before making the next call
    }
  }
}
