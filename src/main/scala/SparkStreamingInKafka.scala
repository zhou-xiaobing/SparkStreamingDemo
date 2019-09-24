import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
/**
  * @Description
  * @Author Zhou-Xiaobing
  * @Date 2019/9/19 10:48
  */
object SparkStreamingInKafka {
  def main(args: Array[String]): Unit = {
    //离线任务是创建SparkContext，现在要实时计算，用StreamingContext
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("SparkStreamingInKafkaDemo")
    val sc = new SparkContext(sparkConf)
    //StreamingContext是对SparkContext的包装，第一个参数是SparkContext，第二个参数是每个批次的时间间隔
    val streamingContext = new StreamingContext(sc,Milliseconds(5000));
    //利用StreamingContext就可以创建SparkStreaming的抽象——DStream
    //socketStream从一个tcp端口读取数据
    val lines: ReceiverInputDStream[String] = streamingContext.socketTextStream("ambari-3",8888)
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
