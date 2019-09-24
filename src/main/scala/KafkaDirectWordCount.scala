import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Duration, StreamingContext}
import org.apache.spark.{SparkConf, TaskContext}

import scala.collection.immutable

/**
  * @Description kafka直连方式，在zookeeper中记录topic+group的偏移量，程序每次重启从上次结束的位置开始读取消息
  * @Author Zhou-Xiaobing
  * @Date 2019/9/19 18:18
  */
object KafkaDirectWordCount {
  def main(args: Array[String]): Unit = {

    //组名
    val groupId = "group001"

    //创建sparkConf
    val sparkConf = new SparkConf().setAppName("KafkaDirectWordCount").setMaster("local[2]")

    //创建sparkStreaming并设置批次的时间间隔
    val streamingContext = new StreamingContext(sparkConf,Duration(5000))

    //指定消费的topic
    val topic = "wordCount"

    //指定kafka的broker地址（sparkStream的task直接连到kafka的分区上）
    var brokerList = "ambari-1:6667"

    //指定zk的地址，用来记录后期更新消费的偏移量（可以使用redis或mysql记录偏移量）
    var zkQuorum = "ambari-1:2181,ambari-2:2181,ambari-3:2181"

    //sparkStrreaming可以同时消费多个topic
    val topics: Set[String] = Set(topic)

    //创建一个ZKGroupTopicDirs对象（topic和group作为具体消息的唯一标识）
    //其实是指定往zk中写入数据的目录，用来保存偏移量
    //zookeeper中的路径 "/group001/offsets/wordCount/"
    //val topicDirs = new ZKGroupTopicDirs(topic,groupId)

     //kafka相关参数
     val kafkaParams = Map[String, Object](
       "bootstrap.servers" -> "ambari-1:6667",
       "key.deserializer" -> classOf[StringDeserializer],
       "value.deserializer" -> classOf[StringDeserializer],
       "group.id" -> groupId,
       "auto.offset.reset" -> "latest",
       "enable.auto.commit" -> (false: java.lang.Boolean)
     )

    //创建DStream
    val stream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      streamingContext,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

    //记录全局偏移量
    var offsetRanges = Array[OffsetRange]()

    //该transform方法计算获取到当前批次RDD的偏移量
//    val transform: DStream[ConsumerRecord[String, String]] = stream.transform(rdd => {
//      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
//      rdd
//    }
//    )
//
//    val messages: DStream[String] = transform.map(_.value())
//
//    //迭代DStream中的RDD
//    messages.foreachRDD(rdd => {
//      rdd.foreachPartition( partition => {
//        partition.foreach( x => {
//          println(x)
//        })
//      })
//    })

    //将kafkaRDD造型之后拿到偏移量，再拿到DStream中的RDD，对RDD进行foreachPartition
    stream.foreachRDD( kafkaRDD => {
      //直连方式只有在KafkaDStream的RDD中才能获取偏移量
      //所以只能在KafkaDStream调用foreachPartition，获取RDD的偏移量，然后就是对RDD的操作了
      val offsetRanges = kafkaRDD.asInstanceOf[HasOffsetRanges].offsetRanges

      val lines: RDD[String] = kafkaRDD.map(_.value())

      val word = lines.flatMap(_.split(" "))

      val wordAndOne = word.map((_,1))

      val reduced = wordAndOne.reduceByKey(_+_)

      val result: RDD[(String, Int)] = reduced.sortBy(_._2)

//      result.collect()

//      //对RDD进行操作，触发action
      result.foreachPartition( partition => {
        partition.foreach( x => {
          println(x)
        })
      })
    })


    //遍历DStream中的RDD，获取RDD中的偏移量，再遍历RDD的每个分区
//    stream.foreachRDD( rdd => {
//      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
//      rdd.foreachPartition( iter => {
//        val o: OffsetRange = offsetRanges(TaskContext.get.partitionId)
//        println(s"${o.topic} ${o.partition} ${o.fromOffset} ${o.untilOffset}")
//      })
//      stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
//    }
//    )
    //必须启动sparkStreaming，程序才会开始运行
    streamingContext.start()
    //等待优雅的退出
    streamingContext.awaitTermination()
  }
}
