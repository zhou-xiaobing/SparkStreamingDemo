package offsetInMysql

import java.sql.DriverManager

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010.OffsetRange

/**
  * @Description
  * @Author Zhou-Xiaobing
  * @Date 2019/9/24 15:10
  */
object OffsetManager {
  //读取配置文件
  val config: Config = ConfigFactory.load()

  //数据库连接参数配置
  def getConn = {
    DriverManager.getConnection(
      config.getString("db.url"),
      config.getString("db.user"),
      config.getString("db.password")
    )
  }


  /*
  获取偏移量信息
   */
  def apply(groupId: String, topic: String) = {
    val conn = getConn
    val statement = conn.prepareStatement("select * from Offset where groupid=? and topic=?")
    statement.setString(1, groupId)
    statement.setString(2, topic)
    val rs = statement.executeQuery()
    //注意导包
    import scala.collection.mutable._
    val offsetRange = Map[TopicPartition, Long]()
    while (rs.next()) {

      //讲获取的数据放到map中
      offsetRange += new TopicPartition(rs.getString("topic"), rs.getInt("partitioner")) -> rs.getLong("offset")
    }
    rs.close()
    statement.close()
    conn.close()
    offsetRange
  }

  /*
  保存当前偏移量到数据库
   */
  def saveCurrentBatchOffset(groupId: String, offsetRange: Array[OffsetRange]) = {
    val conn = getConn
    val statement = conn.prepareStatement("insert into Offset values(?,?,?,?)")
    for (i <- offsetRange) {
      statement.setString(1, groupId)
      statement.setString(2, i.topic)
      statement.setInt(3, i.partition)
      statement.setLong(4, i.untilOffset)
      statement.executeUpdate()

    }

    statement.close()

    conn.close()

  }

}
