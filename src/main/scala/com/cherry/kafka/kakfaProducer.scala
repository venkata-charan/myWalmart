package com.cherry.kafka

import java.util.Properties
import java.util.Date

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.errors.{AuthorizationException, OutOfOrderSequenceException, ProducerFencedException}

import scala.util.Random

object kakfaProducer extends App {

  val events = args(0).toInt
  val brokerip = args(1)
  val topic = args(2)
  val rnd = new Random()

  val props: Properties = new Properties()
  props.put("bootstrap.servers", brokerip)
  props.put("client.id", "ScalaProducerExample")
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

  val myProducer = new KafkaProducer[String, String](props)



  try {
    for (i <- Range(0, events)) {
      val tim = new Date().getTime
      val ip = "192.34.233." + rnd.nextInt(255)
      val msg = tim + "," + i + "www.some" + rnd.nextInt(255) + ".com, " + ip
      val data = new ProducerRecord[String, String](topic, ip, msg)
      myProducer.send(data)
    }
  } catch {
    case e: ProducerFencedException =>
      println("Sorry not able to send data " + e)
      myProducer.close()

    case e: OutOfOrderSequenceException =>
      println("Sorry not able to send data " + e)
      myProducer.close()

    case e: AuthorizationException =>
      println("Sorry not able to send data " + e)
      myProducer.close()

    case e: KafkaException =>
      println("full jambal hot ra reiii " + e)
      myProducer.abortTransaction()

  }

}
