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

import play.api.libs.json.{JsValue, Json}

object SensorProcessor{
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

    val hotSensors = tempGroupedById.filter( sensorAndReadings => sensorAndReadings._2 forall (_._1 > 50) )

    val hotSensorCount = hotSensors.count()

    // Write each hotSensor to kafka topic called "hotSensor"
    val producerConf = new Properties()
    producerConf.put("serializer.class", "kafka.serializer.StringEncoder")
    producerConf.put("metadata.broker.list", metadataBrokerList)
    producerConf.put("request.required.acks", "1")
    hotSensors.writeToKafka(producerConf,
                  hotSensorRecord => new KeyedMessage[String, String]("hotSensor", Json.toJson(hotSensorRecord._1).toString))

    hotSensorCount.print()
    sensorsInMicrobatch.print()

    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }

  def parseJson(jsonEvent: String): (Int, (Double,String)) = {
    val json: JsValue = Json.parse(jsonEvent)

    val sensorId = (json \ "sensorId").as[Int]
    val temp = (json \ "temp").as[Double]
    val descText = (json \ "desc").as[String]

    (sensorId, (temp, descText))
  }
}

