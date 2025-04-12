import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

object KafkaSparkStreaming {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("Tarea3_Grupo16")
      .master("local[*]")
      .getOrCreate()
      
    val ssc = new StreamingContext(spark.sparkContext, Seconds(5))
    
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "group.id" -> "spark-streaming-group",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    
    val topics = Array("eventos-tiempo-real")
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )
    
    // Procesamiento de los mensajes
    val events = stream.map(record => record.value)
    
    // Contar eventos por tipo
    val eventCounts = events
      .map(record => {
        import org.json4s._
        import org.json4s.jackson.JsonMethods._
        implicit val formats = DefaultFormats
        parse(record).extract[Map[String, Any]]
      })
      .map(data => (data("event").toString, 1))
      .reduceByKey(_ + _)
    
    // Mostrar resultados
    eventCounts.print()
    
    ssc.start()
    ssc.awaitTermination()
  }
}
