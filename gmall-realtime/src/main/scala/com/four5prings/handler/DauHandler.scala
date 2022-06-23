package com.four5prings.handler

import com.four5prings.bean.StartUpLog
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis

import java.{lang, util}
import java.text.SimpleDateFormat
import java.util.Date

/**
 * @ClassName DauHandler
 * @Description 实现一些业务需求中的复杂方法，实现解耦
 * @Author Four5prings
 * @Date 2022/6/23 15:30
 */
object DauHandler {
  def saveToRedis(filterByGroupDStream: DStream[StartUpLog]) = {
    //这里需要注意，DStream中rdd的计算都是在executor端，而其他诸如创建连接等操作在dirver端，而连接无法序列化
    //所以需要在executor端创建连接，这里使用含分区类的算子/原语可以节省创建连接个数
    filterByGroupDStream.foreachRDD(rdd =>{
      rdd.foreachPartition(partition => {
        //创建连接
        val jedis: Jedis = new Jedis("hadoop102", 6379)
        //写入数据
        partition.foreach(log =>{
          val redisKey: String = "DAU:" + log.logDate
          jedis.sadd(redisKey,log.mid)
        })
        //关闭连接
        jedis.close()
      })
    })
  }

  def filterByGroup(filterByRedisDStream: DStream[StartUpLog]):DStream[StartUpLog] = {
    //批次内去重，我们可以使用groupbykey的方式，设置一个key，就是一个mid+date，value是原数据
    //这样相同的key聚合在一起后，我们按照ts进行排序，只拿取第一个数
    // 1. 转换为kv的形式
    val midAndDateToLogDStream: DStream[((String, String), StartUpLog)] = filterByRedisDStream.mapPartitions(partition => {
      partition.map(log => {
        ((log.mid, log.logDate), log)
      })
    })
    
    // 2. 进行groupbykey
    val midAndDateToLogIterDStream: DStream[((String, String), Iterable[StartUpLog])] = midAndDateToLogDStream.groupByKey()

    // 3. 按照ts排序，
    val midWithLogDataToListLogDStream: DStream[((String, String), List[StartUpLog])] = midAndDateToLogIterDStream.mapValues(iter => {
      iter.toList.sortBy(_.ts).take(1)
    })
    //取出value，即我们的log数据
    midWithLogDataToListLogDStream.flatMap(_._2)
  }

  //批次间去重
  def filterByRedis(startUpLogDStream: DStream[StartUpLog],sc:SparkContext):DStream[StartUpLog] = {
    //批次间去重，那么就是遍历这个DStream，查看redis中是否有这个value，即sismembers方法
    //方案一  直接过滤,创建了太多的连接
    /*val value: DStream[StartUpLog] = startUpLogDStream.filter(log => {
      //创建连接
      val jedis: Jedis = new Jedis("hadoop102", 6379)

      //通过log拿取其rediskey以及 对比是否存在该value
      val redisKey: String = "DAU:" + log.logDate
      val flag: lang.Boolean = jedis.sismember(redisKey, log.mid)

      jedis.close()
      !flag
    })
    value*/

    // 方案二  分区内创建连接 还可以进一步优化
    /*startUpLogDStream.mapPartitions(partition =>{
      //创建连接
      val jedis: Jedis = new Jedis("hadoop102", 6379)

      val logs: Iterator[StartUpLog] = partition.filter(log => {
        //创建rediskey
        val redisKey: String = "DAU:" + log.logDate
        !jedis.sismember(redisKey, log.mid)
      })
      //关闭连接
      jedis.close()
      logs
    })*/

    // 方案三 直接批次间创建连接，使用广播变量
    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    val value: DStream[StartUpLog] = startUpLogDStream.transform(rdd => {
      //创建连接，获取redisKey对应的所有数据，将数据集广播到executor端
      val jedis: Jedis = new Jedis("hadoop102", 6379)
      //问题一，我们无法获取时间，那么直接创建一个当天的日期
      val redisKey: String = "DAU:" + sdf.format(new Date(System.currentTimeMillis()))

      // 通过key获取所有的redis数据集，并通过rdd找到context广播到executor端
      val logs: util.Set[String] = jedis.smembers(redisKey)
      val logsBC: Broadcast[util.Set[String]] = sc.broadcast(logs)

      val midFilterRDD: RDD[StartUpLog] = rdd.filter(log => {
        !logsBC.value.contains(log.mid)
      })
      jedis.close()
      midFilterRDD
    })
    value
  }
}
