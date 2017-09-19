package com.daxin

import java.util.Date

import com.alibaba.fastjson.JSON
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, LocationStrategies, KafkaUtils}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkContext, SparkConf}

import scala.collection.mutable

/**
  * Created by Daxin on 2017/9/6.
  */
object Utils {


  //状态函数，抽象到Utils中了
  val updateFunc = (seq: Seq[Seq[Long]], maxTimeAndSumTime: Option[TimeList]) => {
    //op: Option[(Long, Long)]中第一个是上一个批次的最大时间，第二个是历史数据的容器
    val timeList = maxTimeAndSumTime.getOrElse(new TimeList())


    //展平list
    val highAndEqual = seq.flatten[Long].:+(timeList.getMaxHistoryTime).filter(x => (x >= timeList.getMaxHistoryTime && x > 0)).sorted

    //low 这一部分有时间补偿，如果将小于的数据也在当前批次计算的话会重复计算
    val low = if (timeList.getMaxHistoryTime != 0) seq.flatten[Long].filter(_ < timeList.getMaxHistoryTime).sorted else List.empty

    var sumTime = 0L
    if (highAndEqual.size > 1) {
      val tail = highAndEqual.tail
      sumTime = highAndEqual.zip(tail).map {
        x =>
          val gap = x._2 - x._1
          //时间间隔大于5分钟则返回0，否则返回实际停留的时间，单位：毫秒
          if (gap > 300000) 0 else gap
      }.sum
    }

    //添加当前批次的停留时间
    timeList.addCurrentBatchStayTime(sumTime)
    //将当前批次数据添加到历史数据容器中
    timeList.addElements(low.toArray)
    timeList.addElementsWithOutTime(highAndEqual.toArray)
    Option(timeList)
  }

  /**
    *
    * @param path  checkpoint path
    * @param topic kafka topic
    * @return
    */
  def createNewStreamingContext(path: String, topic: String): StreamingContext = {

    //Kafka 配置
    val params = new mutable.HashMap[String, String]()
    params.put("bootstrap.servers", "node:9092")
    params.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    params.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    params.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    params.put("group.id", "jd-group");
    params.put("enable.auto.commit", "true");
    params.put("auto.commit.interval.ms", "1000");
    params.put("session.timeout.ms", "30000");
    val topics = Seq(topic)

    //1:配置
    val conf = new SparkConf().setAppName("jd-stream") //.setMaster("local[4]")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    // 注册要序列化的自定义类型。
    conf.registerKryoClasses(Array(classOf[Record], classOf[TimeList]))
    val sc = new SparkContext(conf)
    //TODO 设置时间间隔为5秒
    val ssc = new StreamingContext(sc, Seconds(5))
    //Kafka时候使用Spark CheckPoint机制恢复故障，继续从上次位置消费Kafka（也可以将offset使用外部存储保存，实时更新）
    ssc.checkpoint(path)

    //2：设置数据源
    val dstream = KafkaUtils.createDirectStream(ssc, LocationStrategies.PreferBrokers, ConsumerStrategies.Subscribe[String, String](topics, params))
    //dstream.checkpoint(Seconds(5))

    //3：解析数据，拼接二元组：(shopId+userId，time)
    val pairs = dstream.map {
      x =>
        val obj = JSON.parseObject(x.value(), classOf[Record])
        (obj.getId + "-" + obj.getMac, new Date(obj.getTime).getTime)
    }

    val state = pairs.groupByKey().map {
      x =>
        (x._1, x._2.toSeq)
    }.updateStateByKey(Utils.updateFunc)




    state.foreachRDD {
      rdd =>
        rdd.foreachPartition {
          //资源池获取连接池
          lazy val redis = RedisConnectionsPool.getJedis(5)
          part => part.foreach {
            record =>
              val key = record._1
              val value = record._2
              redis.set(key, value.getStayTime.toString)
          }
            //将连接放回redis
            redis.close()
        }
    }


    ssc

  }


}
