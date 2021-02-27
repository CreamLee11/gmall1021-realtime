package com.atguigu.gmall1021.realtime.app

import java.lang
import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.gmall1021.realtime.app.util.MyKafkaUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

object DauApp {

  def main(args: Array[String]): Unit = {
      //1  sparkstreaming 要能够消费到kafka
       val sparkConf: SparkConf = new SparkConf().setAppName("dau_app").setMaster("local[4]")
       val ssc = new StreamingContext(sparkConf,Seconds(5))
    //  2 通过 工具类 获得 kafka数据流
       val topic="ODS_BASE_LOG"
       val groupId="dau_group"
       val inputDstream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(topic,ssc,groupId)
     //  inputDstream.map(_.value()).print(1000)

    // 3 统计用户当日的首次访问 dau uv
    //    1 可以通过判断 日志中page栏位是否有 last_page_id来决定该页面为 -->  一次访问会话的首个页面
    //    2 也可以通过启动日志来判断  是否首次访问
    //先转换格式，转换成方便操作的jsonObj
    val logJsonDstream: DStream[JSONObject] = inputDstream.map { record =>
      val jsonString: String = record.value()
      val logJsonObj: JSONObject = JSON.parseObject(jsonString)
      // 把ts 转换 成日期 和小时
      val ts: lang.Long = logJsonObj.getLong("ts")
      val simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH")
      val dateHourString: String = simpleDateFormat.format(new Date(ts))
      val dt=  dateHourString.split(" ")(0)
      val hr= dateHourString.split(" ")(1)
      logJsonObj.put("dt",dt)
      logJsonObj.put("hr",hr)
      logJsonObj
    }


   //过滤  得到每次会话的第一个访问页面
    val firstPageJsonDstream: DStream[JSONObject] = logJsonDstream.filter { logJsonObj =>
      var isFirstPage = false
      //有page元素，但是page元素中没有last_page_id的 要留（ 返回true） 其他的过滤掉（返回false)
      val pageJsonObj: JSONObject = logJsonObj.getJSONObject("page")
      if (pageJsonObj != null) {
        val lastPageId: String = pageJsonObj.getString("last_page_id")
        if (lastPageId == null) {
          isFirstPage = true
        }
      }
      isFirstPage
    }
    //
//    firstPageJsonDstream.transform { rdd =>
//      println("过滤前" + rdd.count())
//      rdd
//    }




    //   要把每次会话的首页 ---去重---> 当日的首次访问（日活）
    //   1如何去重：  本质来说就是一种识别  识别每条日志的主体(mid)  对于当日来说是不是已经来过了
    //   2如何保存用户访问清单 ： a  redis
    //       b updateStateByKey  checkpoint 弊端 1不方便管理 2 容易非常臃肿     [mapStateBykey experiment]
    //   3如何把每天用户访问清单保存在redis中
    //     type?     set      key? dau:2021-02-27    value ? mid        读写api ?  sadd  即做了判断又执行了写入
    //     过期时间？ 24小时
  //   type ? string
    val dauJsonDstream: DStream[JSONObject] = firstPageJsonDstream.filter { logJsonObj =>
      val jedis = new Jedis("hdp1", 6379)
      val dauKey = "dau:" + logJsonObj.getString("dt") // 设定key 每天一个清单 所以每个日期有一个key
      val mid: String = logJsonObj.getJSONObject("common").getString("mid")//从json中取得mid
      val isFirstVisit: lang.Long = jedis.sadd(dauKey, mid) // sadd 如果清单中没有 返回1 表示新数据  返回0 表示清单中已有  舍弃
      if (isFirstVisit == 1L) {
        println("用户："+mid+"首次访问 保留")
        true
      } else {
        println("用户："+mid+"已经重复，去掉")
        false
      }
    }
    dauJsonDstream.print(1000)


       ssc.start()
       ssc.awaitTermination()

  }

}
