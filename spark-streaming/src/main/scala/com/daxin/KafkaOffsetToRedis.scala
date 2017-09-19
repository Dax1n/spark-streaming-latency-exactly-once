package com.daxin

import java.util.Date

import com.alibaba.fastjson.JSON
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010._
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

/**
  * Created by Daxin on 2017/9/8.
  * <br><br>
  * 和Utils中的方法业务逻辑一样，就是本类中的支持offset的管理
  *
  *
  */
object KafkaOffsetToRedis {
  /**
    *
    * @param path  checkpoint path
    * @param topic kafka topic
    * @return
    */
  def createNewStreamingContext(path: String, topic: String): StreamingContext = {

    //1：Kafka 配置
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

    //2:配置Spark
    val conf = new SparkConf().setAppName("jd-stream").setMaster("local[4]")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    // 注册要序列化的自定义类型。
    conf.registerKryoClasses(Array(classOf[Record], classOf[TimeList]))
    val sc = new SparkContext(conf)
    //TODO 设置时间间隔为5秒
    val ssc = new StreamingContext(sc, Seconds(5))
    //Kafka时候使用Spark CheckPoint机制恢复故障，继续从上次位置消费Kafka（也可以将offset使用外部存储保存，实时更新）
    ssc.checkpoint(path)

    //3：获取Redis连接
    val redis = RedisConnectionsPool.getJedis(2)
    //4：加载上次消费的位置
    val offsetConfig = mutable.HashMap[TopicPartition, Long]()
    val keySet = redis.keys("*").iterator()
    while (keySet.hasNext) {
      val key = keySet.next() //jd-kafka-topic-5-1 ，以后一定注意kafka的topic名字
      val topicName = key.substring(0, key.length - 2)
      val partitionId = key.substring(key.length - 1, key.length).toInt
      val offset = redis.get(key).toLong

      println(key + "   -----   " + offset)
      offsetConfig(new TopicPartition(topicName, partitionId)) = offset
    }

    //返回连接池
    redis.close()

    //5：根据上次的唯一消费数据。ConsumerStrategies可以定义消费的其实位置
    val dstream = KafkaUtils.createDirectStream(ssc, LocationStrategies.PreferBrokers, ConsumerStrategies.Subscribe[String, String](topics, params, offsetConfig))


    //6：解析数据，拼接二元组：(shopId+userId，time)
    val pairs = dstream.map {
      x =>
        val obj = JSON.parseObject(x.value(), classOf[Record])
        (obj.getId + "-" + obj.getMac, new Date(obj.getTime).getTime)
    }

    val state = pairs.groupByKey().map {
      x =>
        (x._1, x._2.toSeq)
    }.updateStateByKey(Utils.updateFunc)



    //手动存储Kafka的消费offset
    dstream.foreachRDD {
      // 此处代码都是在Driver执行，没有在worker执行了，所有没有优化空间
      rdd =>
        val kafakRedis = RedisConnectionsPool.getJedis(2)
        val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        offsetRanges.foreach {
          offsetRange =>
            val current = offsetRange.untilOffset //该kafka
          val topicAndPartitionId = offsetRange.topicPartition().toString
            kafakRedis.set(topicAndPartitionId, current + "")

        }
        kafakRedis.close()
    }

    ssc

  }

}
