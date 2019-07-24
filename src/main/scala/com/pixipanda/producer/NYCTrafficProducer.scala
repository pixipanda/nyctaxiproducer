package com.pixipanda.producer

import java.io.{BufferedReader, FileReader}
import java.util.{Properties, Random}

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}


class  NYCTrafficProducer (brokers:String, topic:String, taxifile:String) extends  Thread{

  val props = createProducerConfig(brokers)
  val producer = new KafkaProducer[String, String](props)


  def createProducerConfig(brokers: String) = {
    val props = new Properties()
    props.put("bootstrap.servers", brokers)
    /*    props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);*/
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props
  }


  def publish(): Unit =  {

    val rand: Random = new Random
    val br = new BufferedReader(new FileReader(taxifile))

    var line = br.readLine()
    println("line: " + line)
    while(line != null) {
      val producerRecord = new ProducerRecord[String, String](topic, line)
      producer.send(producerRecord)
      line = br.readLine()
      Thread.sleep(rand.nextInt(3000 - 1000) + 1000)

      println("line: " + line)
    }
    br.close()
    producer.close()
  }

  override def run() = {

    publish()
  }
}


object NYCTrafficProducer {

  def main(args: Array[String]) {

    if (args.length != 4) {
      println("Please provide command line arguments: ridestopic farestopic, taxiridesfile and taxifaresfile path")
      System.exit(-1)
    }

    val ridestopic = args(0)
    val farestopic = args(1)
    val ridesFile = args(2)
    val faresFile = args(3)

    val nycTaxiRidesProducerThread = new NYCTrafficProducer("localhost:9092", ridestopic, ridesFile)
    nycTaxiRidesProducerThread.start()
    val nycTaxiFaresProducerThread = new NYCTrafficProducer("localhost:9092", farestopic, faresFile)
    nycTaxiFaresProducerThread.start()

  }
}
