package com.daxin

import org.apache.spark.streaming.StreamingContext


/** ;
  *
  * @author Daxin
  */
object ConsumeFromKafka {


  def main(args: Array[String]) {

    if (args.length==0) {
      println("the program exit because please input your kafka topic and try again .")
      System.exit(0)
    }

    val path = "C:\\Data\\checkpointing"
    val topic = args(0)
    System.setProperty("hadoop.home.dir", "C:\\hadoop-2.7.2");
    val ssc = StreamingContext.getOrCreate(path, () => KafkaOffsetToRedis.createNewStreamingContext(path, topic))
    ssc.start()
    ssc.awaitTermination()

  }


}
