package com.four5prings.app

import com.alibaba.fastjson.JSON
import com.four5prings.bean.OrderInfo
import com.four5prings.constants.GmallConstants
import com.four5prings.utils.MyKafkaUtil
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.phoenix.spark._

/**
 * @ClassName GmvApp
 * @Description
 * @Author Four5prings
 * @Date 2022/6/24 12:07
 */
object GmvApp {
  def main(args: Array[String]): Unit = {
    //创建sparkconf
    val conf: SparkConf = new SparkConf().setAppName("GmvApp").setMaster("local[*]")
    //创建StreamingContext
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(3))
    //获取kafkaDStream流
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_ORDER, ssc)

    //将KafkaDstream中的json转化为样例类
    val orderInfoDStream: DStream[OrderInfo] = kafkaDStream.mapPartitions(partition => {
      partition.map(record => {
        val orderInfo: OrderInfo = JSON.parseObject(record.value(), classOf[OrderInfo])
        orderInfo.create_date = orderInfo.create_time.split(" ")(0)
        orderInfo.create_hour = orderInfo.create_time.split(" ")(1).split(":")(0)
        orderInfo
      })
    })
    //将数据存入Hbase
    orderInfoDStream.foreachRDD(rdd =>{
      rdd.saveToPhoenix(
        "GMALL2022_ORDER_INFO",
        Seq("ID", "PROVINCE_ID", "CONSIGNEE", "ORDER_COMMENT", "CONSIGNEE_TEL", "ORDER_STATUS", "PAYMENT_WAY", "USER_ID", "IMG_URL", "TOTAL_AMOUNT", "EXPIRE_TIME", "DELIVERY_ADDRESS", "CREATE_TIME", "OPERATE_TIME", "TRACKING_NO", "PARENT_ORDER_ID", "OUT_TRADE_NO", "TRADE_BODY", "CREATE_DATE", "CREATE_HOUR"),
        HBaseConfiguration.create(),
        zkUrl = Some("hadoop102,hadoop103,hadoop104:2181")
      )
    })

    //开启并阻塞
    ssc.start()
    ssc.awaitTermination()
  }
}
