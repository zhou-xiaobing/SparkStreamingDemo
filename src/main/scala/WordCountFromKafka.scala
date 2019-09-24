import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010._
/**
  * @Description
  * @Author Zhou-Xiaobing
  * @Date 2019/9/19 14:46
  */
object WordCountFromKafka {

  def main(args: Array[String]): Unit = {

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "ambari-1:6667",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "group1",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    val topic = Array("message")

    val sparkConf = new SparkConf().setAppName("KafkaWordCount").setMaster("local[2]")

    val streamingContext = new StreamingContext(sparkConf,Seconds(5))

//    val zkQuorum = "ambari-1:2181,ambari-2:2181,ambari-3:2181"
//
//    val groupId = "group1"
//
//    val topic = Map[String,Int]("message" -> 1)
    //创建DStream，需要kafkaDStream
    val data: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String,String](
        streamingContext,
        PreferConsistent,
        Subscribe[String, String](topic, kafkaParams)
)
    //kafka的ReceiverInputDStream[(String, String)]里面装的是一个元组（key是写入的key，value是实际写入的内容）
    val lines: DStream[String] = data.map(_.value())
    //对DStream进行操作，操作这个抽象（代理、描述）就像操作一个本地的集合
    //切分压平
    val words: DStream[String] = lines.flatMap(_.split(" "))
    val wordAndOne: DStream[(String, Int)] = words.map((_,1))
    val reduced: DStream[(String, Int)] = wordAndOne.reduceByKey(_+_)
    //打印结果(action操作)
    reduced.print()
    //必须启动sparkStreaming，程序才会开始运行
    streamingContext.start()
    //等待优雅的退出
    streamingContext.awaitTermination()
  }
}
