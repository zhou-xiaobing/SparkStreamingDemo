import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.{HashPartitioner, SparkConf}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent

/**
  * @Description
  * @Author Zhou-Xiaobing
  * @Date 2019/9/19 16:11
  */
object WordCountSumFromKafka {

//  /**
//    * 该方法传入一个可迭代的数组，数组里面的元素是一个元组
//    * 第一个参数：聚合的key，就是单词
//    * 第二个参数： 当前批次该单词在每一个分区出现的次数（局部聚合后单词次数的集合，相加得到全局次数）
//    * 第三个参数： 初始值或累加的中间结果
//    * 返回一个元组，key是单词，value是单词在所有分区出现的次数
//    */
//  val updateFunc = (iterator: Iterable[(String,Seq[Int],Option[Int])]) => {
//    iterator.map(t => (t._1,t._2.sum+t._3.getOrElse(0)))
//  }
  val updateFunc = (values: Seq[Int], state: Option[Int]) => {
    val currentCount = values.sum

    val previousCount = state.getOrElse(0)

    Some(currentCount + previousCount)
  }

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

    val streamingContext = new StreamingContext(sparkConf, Seconds(5))

    //因为需要累加历史数据，所以需要把中间结果保存在文件系统中
    streamingContext.checkpoint("./ck")

    //创建DStream，需要kafkaDStream
    val data: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      streamingContext,
      PreferConsistent,
      Subscribe[String, String](topic, kafkaParams)
    )
    //kafka的ReceiverInputDStream[(String, String)]里面装的是一个元组（key是写入的key，value是实际写入的内容）
    val lines: DStream[String] = data.map(_.value())
    //对DStream进行操作，操作这个抽象（代理、描述）就像操作一个本地的集合
    //切分压平
    val words: DStream[String] = lines.flatMap(_.split(" "))
    val wordAndOne: DStream[(String, Int)] = words.map((_, 1))
    val value = wordAndOne.updateStateByKey(updateFunc, new HashPartitioner(streamingContext.sparkContext.defaultParallelism))
    //updateStateByKey方法详解参考上方的updateFunc
//    val value: DStream[(String, Int)] = wordAndOne.updateStateByKey((currValues: Seq[Int], preValue: Option[Int]) => {
//      var currValueSum = 0
//      for (currValue <- currValues) {
//        currValueSum += currValue
//      }
//      Some(currValueSum + preValue.getOrElse(0))
//    })
    //打印结果(action操作)
    value.print()
    //必须启动sparkStreaming，程序才会开始运行
    streamingContext.start()
    //等待优雅的退出
    streamingContext.awaitTermination()
  }
}

