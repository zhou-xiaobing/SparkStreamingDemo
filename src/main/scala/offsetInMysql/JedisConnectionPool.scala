package offsetInMysql

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

/**
  * @Description redis连接池
  * @Author Zhou-Xiaobing
  * @Date 2019/9/23 15:38
  */
object JedisConnectionPool {

  //创建jedis连接池（单例所以用object）
  val config = new JedisPoolConfig()

  //最大连接数
  config.setMaxTotal(20)

  //最大空闲连接
  config.setMaxIdle(10)

  //连接的活跃性检测
  config.setTestOnBorrow(true)

  //创建JedisPool对象，传入config、IP地址、端口号、超时时间、auth
  val pool = new JedisPool(config, "10.0.0.182", 6379, 10000, "devops")

  //定义返回jedis对象犯法
  def getConnection(): Jedis = {
    pool.getResource
  }

  def main(args: Array[String]): Unit = {
    val conn = JedisConnectionPool.getConnection()

//    //获取redis的key的value
//    val r1 = conn.get("xiaoniu")
//
//    println(r1)
//
//    val r2 = conn.incr("xiaoniu")
//
//    println(r2)
//
    val all = conn.keys("*")

    //java集合的隐式转换
    import scala.collection.JavaConversions._
    for (one <- all) {
      println(one + ":" + conn.get(one))
    }

    //返还redis连接
    conn.close()

  }
}
