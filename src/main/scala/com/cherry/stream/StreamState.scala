package com.cherry.stream

import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import kafka.serializer.StringDecoder


object StreamState extends App {

  val conf = new SparkConf().setAppName("Simple Streaming Application").setMaster("local[2]")
  val ssc = new StreamingContext(conf, Seconds(2))
  val sc = ssc.sparkContext
  sc.setLogLevel("ERROR")

  val topicset = Set(args(0))
  val kafkaParams = Map("metadata.broker.list" -> args(1), "enable.auto.commit" -> "true",
    "auto.offset.reset" -> "smallest","fetch.message.max.bytes"->"52428800")

  val messages  =
    KafkaUtils
      .createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicset)

  // Get the lines, split them into words, count the words and print
  val lines = messages.map(_._2)
  val words = lines.flatMap(_.split(","))
  val wordCounts = words.map(x => (x, 1L)).reduceByKey(_ + _)
  wordCounts.print()

  ssc.checkpoint("hdfs:///user/charanrajlv3971/checkpoint/")
  ssc.start()
  ssc.awaitTermination()

}
