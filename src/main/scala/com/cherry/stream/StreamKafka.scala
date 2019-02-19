package com.cherry.stream

import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import kafka.serializer.StringDecoder


object StreamKafka extends App {

  val conf = new SparkConf().setAppName("Simple Streaming Application").setMaster("local[2]")
  val ssc = new StreamingContext(conf,Seconds(1))
  val sc = ssc.sparkContext
  sc.setLogLevel("ERROR")

  val topicset = Set(args(0))
  val kafkaParams = Map("metadata.broker.list" -> args(1))

  KafkaUtils
    .createDirectStream[String,String,StringDecoder,StringDecoder](ssc,kafkaParams,topicset).map(_._2).print()

  ssc.start()
  ssc.awaitTermination()

}
