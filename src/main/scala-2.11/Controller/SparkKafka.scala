package Controller

import Common.SparkConfig
import kafka.serializer.StringDecoder
import org.apache.spark.SparkContext
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import play.api.libs.json.Json

/**
  * Created by Neil on 1/8/17.
  */
object SparkKafka  extends SparkConfig{

  def main(args: Array[String]) {

    val conf =  SparkConfToES
    val sc = new SparkContext(conf)
    val batchIntervl = 15
    val ssc = new StreamingContext(sc, Seconds(batchIntervl))

    val sqlContext = new SQLContext(sc)

    //kafka group
    val group_id = "receiveScanner"
    // kafka topic
    val topic = Map("testStreaming"-> 1)
    // zk connect
    val zkParams = Map(
      "zookeeper.connect" ->"localhost",
      "zookeeper.connection.timeout.ms" -> "10000",
      "group.id" -> group_id)

    // Kafka
    val kafkaConsumer = KafkaUtils.createStream[String,String,StringDecoder,StringDecoder](ssc,zkParams,topic,StorageLevel.MEMORY_ONLY_SER)
    val receiveData = kafkaConsumer.map(_._2 )
    // printer kafka data
    receiveData.print()
    receiveData.foreachRDD{ rdd=>
      val transform = rdd.map{ line =>
        val data = Json.parse(line)
        // play json parse
        val id = (data \ "id").asOpt[Int] match { case Some(x) => x; case None => 0}
        val name = ( data \ "name"  ).asOpt[String] match { case Some(x)=> x ; case None => "" }
        val age = (data \ "age").asOpt[Int] match { case Some(x) => x; case None => 0}
        val address = ( data \ "address"  ).asOpt[String] match { case Some(x)=> x ; case None => "" }
        Row(id,name,age,address)
      }

      val transfromrecive = sqlContext.createDataFrame(transform,schameType)
      import org.apache.spark.sql.functions._
      import org.elasticsearch.spark.sql._
      //filter age < 20 , to ES database
      transfromrecive.where(col("age").<(20)).orderBy(col("age").asc)
        .saveToEs("member/user",Map("es.mapping.id" -> "id"))
    }

  }

  /**
    * dataframe schame
    * */
  def schameType =  StructType(
    StructField("id",IntegerType,false)::
      StructField("name",StringType,false)::
      StructField("age",IntegerType,false)::
      StructField("address",StringType,false)::
    Nil
  )
}
