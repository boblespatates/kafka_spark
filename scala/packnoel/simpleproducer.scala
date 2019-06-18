package packnoel

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

object simpleproducer extends App {

  override def main(args: Array[String]): Unit = {

    val configproducer = new Properties()
    configproducer.put("bootstrap.servers", "localhost:9092")
    configproducer.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    configproducer.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    configproducer.put("client.id", "SampleProducer")
    configproducer.put("acks", "all")
    configproducer.put("retries", new Integer(1))
    configproducer.put("batch.size", new Integer(16384))
    configproducer.put("linger.ms", new Integer(1))
    configproducer.put("buffer.memory", new Integer(255587878))

    val producer = new KafkaProducer[String, String](configproducer)

    for (i <- 1 to 20000) {
      val monrecortosend: ProducerRecord[String, String] = new ProducerRecord("mytopic", "salut Noel" + i)
      producer.send(monrecortosend)
    }

    producer.close()
  }
}
