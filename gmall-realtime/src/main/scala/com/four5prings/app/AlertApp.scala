package com.four5prings.app

import com.alibaba.fastjson.JSON
import com.four5prings.bean.{CouponAlertInfo, EventLog}
import com.four5prings.constants.GmallConstants
import com.four5prings.utils.{MyEsUtil, MyKafkaUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.util.control.Breaks._
import java.text.SimpleDateFormat
import java.util
import java.util.Date

/**
 * @ClassName AlertApp
 * @Description
 * @Author Four5prings
 * @Date 2022/6/27 10:42
 */
object AlertApp {
  def main(args: Array[String]): Unit = {
    /**
     * 同一设备，5分钟内三次及以上用不同账号登录并领取优惠劵，并且过程中没有浏览商品。
     * 达到以上要求则产生一条预警日志。并且同一设备，每分钟只记录一次预警。
     * 分析 ： 这里的预警日志，要求 5min内 同一设备，即mid相同，三个不同账号，就是uid。
     * 即 一个窗口内 按照mid聚合，转化为(mid，log)
     * 对 每个聚合的mid 的 value进行处理，最终我们是要找到目标数据存入 预警日志的样例类。
     * 这里有两种方式 1. 将log过滤后存入 2.将数据都存入样例类，然后再进行过滤。因为过滤的目标是mid，我们已经按照mid聚合。不符合条件的筛选即可
     * 1分钟只记录一次预警日志，这里我们可以使用es的幂等性，即文档id 设置为mid+精确到分钟的日期。这样就会只保留一条数据
     */
    //1. 创建sparkconf
    val conf: SparkConf = new SparkConf().setAppName("AlertApp").setMaster("local[*]")
    //2. 创建streamingcontext
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(3))
    //3. 获取kafkadstream
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_EVENT, ssc)

    //4. 转换为样例类,因为要聚合，这里可以直接将mid拿出来，变成一个二元组
    val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH")
    val midToLogDStream: DStream[(String, EventLog)] = kafkaDStream.mapPartitions(partition => {
      partition.map(record => {
        val eventLog: EventLog = JSON.parseObject(record.value(), classOf[EventLog])

        val tims: String = sdf.format(new Date(eventLog.ts))
        eventLog.logDate = tims.split(" ")(0)
        eventLog.logHour = tims.split(" ")(1)
        (eventLog.mid, eventLog)
      })
    })

    //5. 开窗
    val midToWindowDStream: DStream[(String, EventLog)] = midToLogDStream.window(Seconds(300))

    //6. 分组聚合mid
    val midToLogIterDStream: DStream[(String, Iterable[EventLog])] = midToWindowDStream.groupByKey()

    //7. 筛选数据，使用map转换，这里采用了标志位删除，并不是直接过滤，而是将数据转换为二元组，_.1 是boolean值方便下一步的过滤筛选
    /**
     * addFavor	点赞
     * addComment	评论
     * addCart	添加购物车
     * clickItem	浏览商品
     * coupon	优惠券
     * 这里的数据都是 按照mid进行分组的，包括了该设备的事件日志，接下来进行筛选
     * 条件如下  ： 1. 三次及以上  就是userid 的个数是 大于等于3
     * 2. 领取优惠劵 有领取优惠券的操作 coupon
     * 3. 不浏览商品  clickItem事件不存在
     * 问题 1： 如何对userid进行去重
     * 2： 何时进行数据存储，是先将所有的数据全部存储到预警日志中，然后再筛选出。还是先对mid进行筛选。
     * 这里 userid的去重，可以使用set集合 进行去重，而预警日志的存储格式也需要使用集合存储。
     * 那么我们可以 直接先将数据进行存储，然后使用标记位删除的方式，存放一个falg：Boolean
     * 并在数据转换为 二元组的形式  （falg，log）
     * 之后预警日志部分，直接使用filter过滤
     */
    val boolDStream: DStream[(Boolean, CouponAlertInfo)] = midToLogIterDStream.mapPartitions(iter => {
      iter.map { case (mid, iter) =>
        // 创建存储 uids，涉及的商品 itemids ， 以及 events
        val uids: util.HashSet[String] = new util.HashSet[String]()
        val itemIds: util.HashSet[String] = new util.HashSet[String]()
        val events = new util.ArrayList[String]()

        /*var flag: Boolean = true
        breakable{
          //遍历 iter 对其中的数据进行处理
          iter.foreach(log => {
            events.add(log.evid)
            if ("coupon".equals(log.evid)) {
              uids.add(log.uid)
              itemIds.add(log.itemid)
            } else if ("clickItem".equals(log.evid)) {
              flag = false
              break()
            }
          })
      }
        ((uids.size() >= 3 && flag), CouponAlertInfo(mid, uids, itemIds, events, System.currentTimeMillis()))
      }*/
        //标志位
        var bool = true
        //判断有没有浏览商品行为
        breakable {
          iter.foreach(log => {
            events.add(log.evid)

            if (log.evid.equals("clickItem")) { //判断用户是否有浏览商品行为
              bool = false
              break()
            } else if (log.evid.equals("coupon")) {  //判断用户是否有领取购物券行为
              itemIds.add(log.itemid)
              uids.add(log.uid)
            }
          })
        }
        //产生疑似预警日志
        ((uids.size() >= 3&& bool), CouponAlertInfo(mid, uids, itemIds, events, System.currentTimeMillis()))
      }
    })

    //8. 生成预警日志
    val alertDStream: DStream[CouponAlertInfo] = boolDStream.filter(_._1).map(_._2)
    alertDStream.print()

    //9. 批量存入es,写库操作,写库是要创建一个id，用来去重，id就是mid+精确到分钟的时间
    alertDStream.foreachRDD(rdd =>{
      rdd.foreachPartition(iter =>{

        //创建一个indexname，因为这里是使用的索引模板
        val indexName: String = GmallConstants.ES_ALERT_INDEXNAME + sdf.format(new Date(System.currentTimeMillis())).split(" ")(0)
        val list: List[(String, CouponAlertInfo)] = iter.toList.map(alter => {
          (alter.mid + alter.ts/1000/60, alter)
        })
        MyEsUtil.insertBulk(indexName,list)
      })
    })

    //启动并阻塞
    ssc.start()
    ssc.awaitTermination()
  }
}
