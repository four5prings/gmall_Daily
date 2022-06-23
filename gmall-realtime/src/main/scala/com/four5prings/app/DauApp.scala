package com.four5prings.app

import com.alibaba.fastjson.JSON
import com.four5prings.bean.StartUpLog
import com.four5prings.constants.GmallConstants
import com.four5prings.handler.DauHandler
import com.four5prings.utils.MyKafkaUtil
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.phoenix.spark._

import java.text.SimpleDateFormat
import java.util.Date

/**
 * @ClassName DauApp
 * @Description
 * @Author Four5prings
 * @Date 2022/6/23 14:59
 */
object DauApp {
  def main(args: Array[String]): Unit = {
    // 创建sparkconf配置
    val conf: SparkConf = new SparkConf().setAppName("DauApp").setMaster("local[*]")

    //创建 ssc
    val ssc = new StreamingContext(conf, Seconds(3))

    //获取kafka流 -这里为了解耦，我们可以创建一个常量类，专门存放kafkatopic
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] =
      MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_STARTUP, ssc)

    /**
     * 获取kafka数据后，现在需要对数据进行处理后存入Hbase数据库中，以方便下一步的操作
     * 要点：
     * 1 .首先kafkaDStream中的数据是一个个DStream，内部是一个个的rdd且是json格式的，
     * json格式不方便处理我们需要先将数据全部转换为样例类，这样方便存储数据，这里的样例类需根据数据格式创建，
     * 2 .指标分析，这里要求的是实时数据，指标为每天的日活总数和每小时的日活，因为数据传输和streaming实时计算，最后由hbase实时处理，
     * 所以数据会实时更新，我们需要先将数据处理后存入hbase，sql阶段是hbase处理，这里需要将数据进行清洗，即去重。来满足日活需求
     * 3 .根据DStream的特性，是微批次，我们设置了是3s一个批次，这里面是一个个rdd，所以存在批次内即rdd之间的去重，和批次间的去重，
     * 分析出我们需要先进行批次间去重，批次间去重，就需要将已经去重过的数据存放到一个数据库中，让新的批次与这个数据库的所有数据进行去重。
     * 采用redis这样一个存在内存中的NoSQL数据库
     * 4 .对批次间进行去重需要利用到redis，需要确定存储在redis中的存储方式及k-v。明确存储的是什么，存储的value就是我们要去重的
     * 即 mid，那么key应该是什么呢，日活，为了区分应该是当天的日期，但是还有其他的指标所以我们可以把key设置为 DAU+日期
     *
     */
    // 将kafka流中的数据转换为样例类，并添加时间字段
    val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH")

    val startUpLogDStream: DStream[StartUpLog] = kafkaDStream.mapPartitions(partition => {
      partition.map(record => {
        //使用alibab的json解析kafkajson格式并转换为相应的类
        val startUpLog: StartUpLog = JSON.parseObject(record.value(), classOf[StartUpLog])

        val time: String = sdf.format(new Date(startUpLog.ts))

        //补充 logdate和loghour两个字段，因为解析格式是可以按照空格切分的
        val strings: Array[String] = time.split(" ")
        startUpLog.logDate = strings(0)
        startUpLog.logHour = strings(1)

        startUpLog
      })
    })

    startUpLogDStream.count().print()
    // 利用redis对数据进行批次间去重,为了解耦，我们创建一个内存放方法
    val filterByRedisDStream: DStream[StartUpLog] = DauHandler.filterByRedis(startUpLogDStream,ssc.sparkContext)

    filterByRedisDStream.count().print()
    // 批次内去重
    val filterByGroupDStream: DStream[StartUpLog] = DauHandler.filterByGroup(filterByRedisDStream)

    filterByGroupDStream.count().print()
    // 将数据存储到redis中
    DauHandler.saveToRedis(filterByGroupDStream)

    //将数据存储到Hbase中，这里使用phoenix存储
    filterByGroupDStream.foreachRDD(rdd => {
      rdd.saveToPhoenix(
        "GMALL2022_DAU",
        Seq("MID", "UID", "APPID", "AREA", "OS", "CH", "TYPE", "VS", "LOGDATE", "LOGHOUR", "TS"),
        HBaseConfiguration.create(),
        Some("hadoop102,hadoop103,hadoop104:2181"),
      )
    })

    //启动并阻塞
    ssc.start()
    ssc.awaitTermination()

  }
}
