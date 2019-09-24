package offsetInMysql

import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}

import scala.collection.mutable
/**
  * @Description kafka直连方式，在zookeeper中记录topic+group的偏移量，程序每次重启从上次结束的位置开始读取消息
  * @Author Zhou-Xiaobing
  * @Date 2019/9/19 18:18
  */
object OrderCount {

  def main(args: Array[String]): Unit = {


    //组名
    val groupId = "group002"

    //创建sparkConf
    val sparkConf = new SparkConf().setAppName("OrderCount").setMaster("local[4]")

    //创建sparkStreaming并设置批次的时间间隔
    val streamingContext = new StreamingContext(sparkConf,Duration(5000))

    //获取ip规则的广播变量（需要指定本地ip规则文件）
    val broadcast: Broadcast[Array[(Long, Long, String)]] = MyUtils.broadcastIpRules(streamingContext,"C:\\Users\\zhoux\\Documents\\ip.txt")

    //指定消费的topic
    val topic = "orders-2"

    //指定kafka的broker地址（sparkStream的task直接连到kafka的分区上）
    var brokerList = "ambari-1:6667"

    //指定zk的地址，用来记录后期更新消费的偏移量（可以使用redis或mysql记录偏移量）
    var zkQuorum = "ambari-1:2181,ambari-2:2181,ambari-3:2181"

    //sparkStrreaming可以同时消费多个topic
    val topics: Set[String] = Set(topic)

     //kafka相关参数
     val kafkaParams = Map[String, Object](
       "bootstrap.servers" -> "ambari-1:6667",
       "key.deserializer" -> classOf[StringDeserializer],
       "value.deserializer" -> classOf[StringDeserializer],
       "group.id" -> groupId,
       //从头的数据开始消费earliest
       "auto.offset.reset" -> "earliest",
       "enable.auto.commit" -> (false: java.lang.Boolean)
     )

    //调用OffsetManager类从数据库中获取TopicPartition对应的偏移量
    val offsetManager: mutable.Map[TopicPartition, Long] = OffsetManager.apply(groupId,topic)

    //判断集合中是否有数据
    var kafkaDStream = if(offsetManager.size>0){
      KafkaUtils.createDirectStream[String,String](
        streamingContext,
        LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe[String,String](topics,kafkaParams,offsetManager)
      )
    }else{
      KafkaUtils.createDirectStream[String,String](
        streamingContext,
        LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe[String,String](topics,kafkaParams)
      )
    }

    //拿到DStream中的RDD，对RDD进行foreachPartition
    kafkaDStream.foreachRDD( kafkaRDD => {
      //获取当前偏移量
      val ranges: Array[OffsetRange] = kafkaRDD.asInstanceOf[HasOffsetRanges].offsetRanges
      //管理偏移量
      OffsetManager.saveCurrentBatchOffset(groupId,ranges)
      //判断当前的KafkaDStream中的RDD是否有数据
      if(!kafkaRDD.isEmpty()) {
        val lines: RDD[String] = kafkaRDD.map(_.value())

        //格式化数据
        val fields: RDD[Array[String]] = lines.map(_.split(" "))

        //计算成交总金额
        CalculateUtils.calculateIncome(fields)

        //计算商品分类金额
        CalculateUtils.calculateItem(fields)

        //计算不同区域的商品总金额
        CalculateUtils.calculateZone(fields, broadcast)
      }
    })
    streamingContext.start()
    //等待优雅的退出
    streamingContext.awaitTermination()
  }
}
