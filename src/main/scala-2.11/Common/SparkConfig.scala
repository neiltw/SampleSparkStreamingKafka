package Common

import org.apache.spark.SparkConf
import org.apache.spark.serializer.KryoSerializer

/**
  * Created by Neil on 1/8/17.
  */
class SparkConfig {

  def SparkConfToES = new SparkConf()
    .setAppName("spark streaming kafka")
    .setMaster("local")
    .set("spark.serializer",classOf[KryoSerializer].getName)
    .set("es.nodes", "192.168.1.33")
    .set("es.port", "9200")

}
