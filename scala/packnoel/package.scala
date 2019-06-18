import java.util.HashMap

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig}
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._

object packnoel {

  def main(args: Array[String]): Unit = {


  val zkQuorum = "localhost:2181" // zookeeper quorum
  val group = "spark-streaming-consumer-group"
  val topic = Map("mytopic" /*nom du topic*/ -> 1)
  val sparkConf = new SparkConf().setAppName("KafkaWordCount")
  val ssc = new StreamingContext(sparkConf, Seconds(2))
  ssc.checkpoint("checkpoint")

  val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topic)
  val words = lines.flatMap(x => x._2.split(" "))
  val wordCounts = words.map(x => (x, 1L))
    .reduceByKeyAndWindow(_ + _, _ - _, Seconds(2))
  wordCounts.print()

  ssc.start()
  ssc.awaitTermination()
  }

}
