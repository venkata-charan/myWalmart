package com.cherry.kafka

import java.time.Duration
import java.util.{Collections, Properties}
import scala.collection.JavaConversions._
import org.apache.kafka.clients.consumer.{ ConsumerRecords, KafkaConsumer}

object kafkaConsumer extends App {


  // get the required parameters to run kafka consumer - topic name and zookeeper instance
  // set the props --> create kafka consumer object and pass props to it
  // subscribe to topics and start reading data , records to record conversion issue

  val topicname = args(0)
  val brokerip = args(1)

  val props: Properties = new Properties()

  props.put("bootstrap.servers", brokerip)
  props.put("group.id", "test")
  props.put("enable.auto.commit", "true")
  props.put("auto.commit.interval.ms", "1000")
  props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")

  val consumer = new KafkaConsumer[String, String](props)
  consumer.subscribe(Collections.singletonList(topicname))

  println(s"Starting Reading messages from $topicname ....................")


  val records:ConsumerRecords[String,String] = consumer.poll(Duration.ofSeconds(60))

  for (record <- records.iterator()) {
    println(record)
  }
  println(s"Reading messages from $topicname completed successfully ...................")
  consumer.close()

}




