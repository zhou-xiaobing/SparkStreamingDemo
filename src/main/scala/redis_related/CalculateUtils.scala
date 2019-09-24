package redis_related

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

/**
  * @Description
  * @Author Zhou-Xiaobing
  * @Date 2019/9/23 16:31
  */
object CalculateUtils {

  //计算所有商品总金额
  def calculateIncome(fields: RDD[Array[String]]) = {
    //将数据计算后写入redis
    val priceRDD = fields.map(_(4).toDouble)
    //获得当前批次的累加金额
    val sum: Double = priceRDD.reduce(_+_)
    //将金额写入到redis中
    //获取redis连接
    val conn = JedisConnectionPool.getConnection()
    //将历史值和当前值累加
    conn.incrByFloat(Constant.TOTAL_INCOME,sum)
    //释放连接
    conn.close()
  }

  //计算不同商品类别的总金额
  def calculateItem(fields: RDD[Array[String]]) = {
    val itemAndPrice: RDD[(String, Double)] = fields.map(array => {
      //分类
      var item = array(2)
      //金额
      var price = array(4).toDouble
      (item, price)
    })
    //商品按不同的分类进行金额的聚合
    val reduced: RDD[(String, Double)] = itemAndPrice.reduceByKey(_+_)
    reduced.foreachPartition(part =>{
      //获取redislianjie
      //这个连接是在executor中创建的
      //JedisConnectionPool在一个executor进程中是单例的
      val conn = JedisConnectionPool.getConnection()
      part.foreach(t => {
        //一个连接更新一个分区的数据
        conn.incrByFloat(t._1,t._2)
      })
      //将当前分区更新完关闭redis连接
      conn.close()
    })
  }

  //计算不同区域的商品总金额
  def calculateZone(fields: RDD[Array[String]],broadcastRef: Broadcast[Array[(Long,Long,String)]]): Unit = {
    val provinceAndPrice = fields.map(arr => {
      var ip = arr(1)
      var price = arr(4).toDouble
      var province = ""
      val ipNum = MyUtils.ip2Long(ip)
      //在executor中获取到广播的全部规则
      val allRules: Array[(Long, Long, String)] = broadcastRef.value
      //二分法查找ip的归属地
      val index = MyUtils.binarySearch(allRules, ipNum)
      if (index != -1) {
        province = allRules(index)._3
      }
      //将province和商品金额封装起来
      (province, price)
    })
    //按省份进行聚合
    val reduced: RDD[(String, Double)] = provinceAndPrice.reduceByKey(_+_)
    //将数据更新到redis中
    reduced.foreachPartition(part => {
      //每个分区获取一个连接
      val conn = JedisConnectionPool.getConnection()
      //遍历分区中的每个RDD，将聚合好的RDD中的数据写入到redis中
      part.foreach(t => {
        conn.incrByFloat(t._1,t._2)
      })
      //关闭redis连接
      conn.close()
    })
  }
}
