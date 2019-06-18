import java.util.Properties
import org.apache.kafka.clients.producer._

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

    for (i <- 1 to 20) {
      val monrecortosend: ProducerRecord[String, String] = new ProducerRecord("matopic", "salut Noel" + i)
      producer.send(monrecortosend)
    }

    producer.close()
  }
}

//telecharger kafka
//#demarer une instance kafka (zookeeper):
//bin/zookeeper-server-start.sh config/zookeeper.properties
//#demarer un broker kafka
//bin/kafka-server-start.sh config/server.properties
//#creation d'un topic 'matopic' sur le brocker
//bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic matopic
//#verification que le topic "test" est bien créé
//bin/kafka-topics.sh --list --bootstrap-server localhost:9092
//#demarer le producer
//bin/kafka-console-producer.sh --broker-list localhost:9092 --topic matopic#tu peux ecrire ici >saut kafka
//#demarer le consumer
//bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic matopic --from-beginning# tu va voir ici: salut kafka