import scala.util.Random
import io.circe.syntax._
import io.circe.generic.auto._

import java.util.Properties
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}


case class Itemsgenered (
                          Curts: Long,
                          X: Int,
                          Y: Int,
                          Z: Int
                        )

object RandomGenerator extends App{

  val props:Properties = new Properties()
  props.put("bootstrap.servers","localhost:29092")
  props.put("key.serializer",
    "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer",
    "org.apache.kafka.common.serialization.StringSerializer")
  props.put("timestamp.serializer",
    "org.apache.kafka.common.serialization.LongSerializer")

  props.put("acks","all")
  val producer = new KafkaProducer[String, String](props)
  val topic = "items_generated"


  try {

    while (true) {

      val xstart = 20
      val xend = 80
      val ystart = 30
      val yend = 60
      val zstart = 20
      val zend = 30
      val selector = Random.nextInt(20)

      val k1 = selector match {
        case 1 => 70
        case 2 => -70
        case 18 => 70
        case 19 => -70
        case _ => 0
      }

      val k2 = selector match {
        case 1 => 40
        case 2 => 40
        case 18 => -40
        case 19 => 40
        case _ => 0
      }

      val k3 = selector match {
        case 1 => 20
        case 2 => 20
        case 18 => -20
        case 19 => -20
        case _ => 0
      }

      val x = xstart + Random.nextInt((xend - xstart) + 1) + k1
      val y = ystart + Random.nextInt((yend - ystart) + 1) + k2
      val z = zstart + Random.nextInt((zend - zstart) + 1) + k3

      val timeInMillis: Long = System.currentTimeMillis()

      val itemrecord = Itemsgenered(timeInMillis, x, y, z).asJson.noSpaces

      val kafkarecord = new ProducerRecord[String, String](topic, itemrecord)
      val metadata = producer.send(kafkarecord)
      printf(s"sent kafkarecord(key=%s value=%s) " +
        "meta(partition=%d, offset=%d)\n",
        kafkarecord.key(), kafkarecord.value(),
        metadata.get().partition(),
        metadata.get().offset())

      Thread.sleep(500)

    }
  }
  catch {
    case e: Exception => e.printStackTrace()
  }
  finally {
    producer.close()
  }

}

