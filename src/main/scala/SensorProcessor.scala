/**
 * To submit this app use the command:
 * spark-submit --class org.cloudera.aiyer.SensorProcessor <other spark yarn params> <name_of_jar>.jar
 *
 * Created by aiyer on 10/18/15.
 */
package org.cloudera.aiyer

import java.util.Properties

import kafka.producer.KeyedMessage
import kafka.serializer.StringDecoder

import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka.KafkaUtils
import org.cloudera.spark.streaming.kafka.KafkaWriter._
import org.json4s.NoTypeHints

// Read the "Serialization" section in https://github.com/json4s/json4s
// Case classes can be serialized and deserialized.
import org.json4s.jackson.Serialization

import org.json4s.NoTypeHints

object SensorProcessor{

  implicit val formats = Serialization.formats(NoTypeHints)

  def main(args: Array[String]): Unit ={

    val metadataBrokerList = "aiyer-ibecoding-3.vpc.cloudera.com:9092"
    val topic = args(0);
    println("Reading from topic " + topic)

    val sparkConf = new SparkConf().setAppName("SensorProcessor")
    val ssc = new StreamingContext(sparkConf, Seconds(1))

    val kafkaParams = Map[String, String]("metadata.broker.list" -> metadataBrokerList)
    val jsonEvents = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topic.split(",").toSet)

    // Convert the incoming json events to key and value, where key is the sensor id, value is the temperature
    val idTempPairs = jsonEvents.map( jsonReading => parseJson(jsonReading._2))

    val windowedIdTempPairs = idTempPairs.window(Seconds(60), Seconds(30))

    windowedIdTempPairs.cache() // By caching it does not recompute window for the two seperate downstream actions

    val tempGroupedById = windowedIdTempPairs.groupByKey()

    val sensorsInMicrobatch = windowedIdTempPairs.count()

    //val tempGroupedById = idTempPairs.groupByKey()
    //val sensorsInMicrobatch = tempGroupedById.count()

    val hotSensors = tempGroupedById.filter( sensorAndReadings => sensorAndReadings._2 forall (_._1 > 10) )

    val hotSensorCount = hotSensors.count()

    // Write each hotSensor to kafka topic called "hotSensor"
    val producerConf = new Properties()
    producerConf.put("serializer.class", "kafka.serializer.StringEncoder")
    producerConf.put("metadata.broker.list", metadataBrokerList)
    producerConf.put("request.required.acks", "1")
    hotSensors.writeToKafka(producerConf,
                  hotSensorRecord => new KeyedMessage[String, String]("hotSensor", hotSensorRecord._1.toString))

    hotSensorCount.print()
    sensorsInMicrobatch.print()

    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }

  def parseJson(jsonInput: String): (Int, (Double,String)) = {
    val reading: SensorReading = Serialization.read[SensorReading](jsonInput)

    (reading.sensorId, (reading.temp, reading.desc))
  }

  //It is silly that I am redefining it here. Ideally i will define it once, put it in a jar and re-use that jar
  //in the producer and the consumer
  case class SensorReading(sensorId: Int, temp: Double, desc: String)}

