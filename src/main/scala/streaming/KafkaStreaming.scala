package streaming


import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.slf4j.LoggerFactory

/**
  * Created by jointwisdom on 2017/8/8 008.
  */
object KafkaStreaming {
  val  logger = LoggerFactory.getLogger(KafkaStreaming.getClass)
  def createContext(checkpointDirectory: String,num:Int): StreamingContext = {
    val sparkConf = new SparkConf().setAppName("KafkaHis")
    val ssc = new StreamingContext(sparkConf, Seconds(num))
    ssc
  }

  def main(args: Array[String]) {
   /* if (args.length < 4) {
      logger.warn("Usage: <broker-list>  <group> <topics> <numSeconds>")
      System.exit(1)
    }*/

//    val checkDir = "./chkDir"
//    val Array(broker,group, topics, numSeconds,lim) = args
//    val ssc =  createContext(checkDir,numSeconds.toInt)
//
    val conf = new SparkConf().setAppName("Spark Stream on Kafka").setMaster("local[2]")
    val streamingContext = new StreamingContext(conf, Seconds(1))

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "10.10.16.103:9092,10.10.16.104:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Array("testkafka", "pms_test")
    val stream = KafkaUtils.createDirectStream[String, String](
      streamingContext,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )
    stream.foreachRDD(rdd =>
      rdd.foreach(record =>
        println(record)
      )
    )
//    stream.map(record => (record.key, record.value))
  }
}
