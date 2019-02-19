package com.cherry.stream

import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import kafka.serializer.StringDecoder


object StreamKafka extends App {

  val conf = new SparkConf().setAppName("Simple Streaming Application")
  val ssc = new StreamingContext(conf,Seconds(1))
  val topicset = Set(args(0))
  val kafkaParams = Map("metadata.broker.list" -> args(1))

  val lines = KafkaUtils
    .createDirectStream[String,String,StringDecoder,StringDecoder](ssc,kafkaParams,topicset).map(_._2)

  lines.foreachRDD((rdd,time)=>rdd.map(x => println("Printing records "+ time + x )))

  ssc.start()
  ssc.awaitTermination()

}
