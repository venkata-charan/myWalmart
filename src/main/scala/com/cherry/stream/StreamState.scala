package com.cherry.stream

import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}
import kafka.serializer.StringDecoder


object StreamState extends App {

  val conf = new SparkConf().setAppName("Simple Streaming Application").setMaster("local[2]")
  val ssc = new StreamingContext(conf, Seconds(10))
  val sc = ssc.sparkContext
  sc.setLogLevel("ERROR")

  val topicset = Set(args(0))
  val kafkaParams = Map("metadata.broker.list" -> args(1), "fetch.message.max.bytes" -> "52428800")

  val lines =
    KafkaUtils
      .createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicset)
      .map(x => (x._1,1))
      .reduceByKey(_+_)
      .repartition(1)
      .print()



//  lines.foreachRDD( rdd => {
//
//    if( rdd.count()>0 ) {
//      println("printing messages from kafka")
//      val repart_Rdd = rdd.repartition(1).cache()
//      repart_Rdd.collect()
//    }
//
//  })

//    .map(x => {
//      val list1 = x._2.split(",")
//      val ip = list1(2)
//      (ip, 1)
//    })
//
//  val runningCounts = lines.updateStateByKey[Int](updateFunction _)
//  runningCounts.print()

  ssc.checkpoint("hdfs:///user/charanrajlv3971/checkpoint/")
  ssc.start()
  ssc.awaitTermination()

  def updateFunction(newValues: Seq[Int], oldStateCount: Option[Int]): Option[Int] = {
    val newCount = newValues.sum
    val previousCount = oldStateCount.getOrElse(0)
    Some(newCount + previousCount)
  }


}
