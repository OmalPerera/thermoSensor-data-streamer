package io.github.omalperera

import java.text.SimpleDateFormat
import java.util.{Calendar, Properties, UUID}

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.util.Random
import scala.util.control.Breaks
import com.google.gson.Gson

//"device_id": 1090011, "ip": "123.231.120.54","city": "C olombo", "latitude": 6.927100, "longitude": 79.861200, "t emp": 31, "timestamp”:1522503549000
case class recordSchema(device_id: String, ip: String, city: String, temp: Integer, batterylevel: String, timestamp: String)

object dataGenerator {
  def main(args: Array[String]): Unit = {

    //default values
    val default_kafkaNode = "localhost:9092"
    val default_kafkaTopics = "thermo5"
    val default_events = "0"
    val default_recordInterval = "1" //in Seconds
    val default_randomStart = "0" //in Seconds
    val default_randomEnd = "500" //in Seconds


    //Assigning values if command line arguments are empty
    val brokers = util.Try(args(0)).getOrElse(default_kafkaNode)
    val topic = util.Try(args(1)).getOrElse(default_kafkaTopics)
    val intervalEvent = util.Try(args(3)).getOrElse(default_recordInterval).toInt


    val events = default_events.toInt
    val randomStart = default_randomStart.toInt
    val randomEnd = default_randomEnd.toInt
    val clientId = UUID.randomUUID().toString()


    //Kafka properties
    val properties = new Properties()
    properties.put("bootstrap.servers", brokers)
    properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    properties.put("client.id", clientId)

    val producer = new KafkaProducer[String, String](properties)

    println("*********** RDS Simulator Started to feed kafka topic '" + topic + "' ***********")

    val citiesInColombo = List("Kollupitiya", "Nugegoda", "Maharagama", "Battaramulla", "Pettah", "Kelaniya", "Moratuwa", "Kotte", "Rajagiriya")
    val iotDeviceID = List("T109001", "T002984", "T119002", "T883645", "T129741", "T009942", "T228742", "T076453", "T112172")
    val ipAddressOfDevices = List("10.112.34.2", "10.53.4.2", "11.68.32.99", "11.22.243.77", "11.32.65.90")
    val batteyLevelOfDevices = List("31%", "53%", "68%", "22%", "65%", "82%", "92%", "15%", "48%", "42%", "85%")
    val tempratureValue = 30
    val rnd = new Random()
    val rnd2 = new Random()

    var i = 0

    val loop = new Breaks()

    //The while loop will generate the data and send to Kafka
    loop.breakable{
      while(true){

        val n = randomStart + rnd2.nextInt(randomEnd - randomStart + 1)
        for(i <- Range(0, n)){

          val today = Calendar.getInstance.getTime
          val formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
          val key = UUID.randomUUID().toString().split("-")(0)
          //val value = "{ 'TimeStamp' :" + formatter.format(today) + ", 'City' :" + citiesInColombo(rnd.nextInt(citiesInColombo.length)) + "}"

          val getrandomDevice = iotDeviceID(rnd.nextInt(iotDeviceID.length))
          val getrandomIpAddress = ipAddressOfDevices(rnd.nextInt(ipAddressOfDevices.length))
          val getrandomCity = citiesInColombo(rnd.nextInt(citiesInColombo.length))
          val getTimeStamp = formatter.format(today)
          val getBatteryLevel = batteyLevelOfDevices(rnd.nextInt(batteyLevelOfDevices.length))
          val getTemprature = tempratureValue

          val jsonRecord = recordSchema(getrandomDevice, getrandomIpAddress, getrandomCity, getTemprature, getBatteryLevel, getTimeStamp)
          val gson = new Gson
          val value = gson.toJson(jsonRecord)


          val data = new ProducerRecord[String, String](topic, key, value)

          //println("--- topic: " + topic + " ---")
          //println("key: " + data.key())
          //println("value: " + data.value() + "\n")
          println(data.value())
          producer.send(data)
        }

        val k = i + 1
        println(s"--- #$k: $n records in [$randomStart, $randomEnd] ---")

        if(intervalEvent > 0)
          Thread.sleep(intervalEvent * 1000)

        i += 1
        if(events > 0 && i == events)
          loop.break()
      }
    }
  }

  }
