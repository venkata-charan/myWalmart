package com.cherry.stream

import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import kafka.serializer.StringDecoder


object StreamState extends App {

  val conf = new SparkConf().setAppName("Simple Streaming Application").setMaster("local[2]")
  val ssc = new StreamingContext(conf, Seconds(1))
  val sc = ssc.sparkContext
  sc.setLogLevel("ERROR")

  val topicset = Set(args(0))
  val kafkaParams = Map("metadata.broker.list" -> args(1))

  val lines =
    KafkaUtils
      .createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicset)
      .map(x => {
        val list1 = x._2.split(",")
        val ip = list1(2)
        (ip, 1)
      })

  val runningCounts = lines.updateStateByKey[Int](updateFunction _)
  runningCounts.print()

  ssc.start()
  ssc.awaitTermination()

  def updateFunction(newValues: Seq[Int], oldStateCount: Option[Int]): Option[Int] = {
    val newCount = newValues.sum
    val previousCount = oldStateCount.getOrElse(0)
    Some(newCount + previousCount)
  }


}
