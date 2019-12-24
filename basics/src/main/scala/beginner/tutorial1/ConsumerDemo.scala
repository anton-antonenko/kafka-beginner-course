package beginner.tutorial1

import java.util.Properties

import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.serialization.StringDeserializer
import org.slf4j.LoggerFactory

import scala.language.postfixOps

object ConsumerDemo extends App {

  val logger = LoggerFactory.getLogger(this.getClass.getSimpleName)

  // set up configs
  val props = new Properties()
  props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
  props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
  props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "my-application")
  props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")


  // create a consumer
  val consumer = new KafkaConsumer[String, String](props)

  // subscribe consumer to topic
  import scala.jdk.CollectionConverters._
  import scala.jdk.DurationConverters._
  import scala.concurrent.duration._

  val topic = "second"
  consumer.subscribe(Seq(topic).asJava)

  // consume records
  while (true) {
    val records = consumer.poll(100.millis.toJava)

    records.forEach(r => {
      logger.info(s"Key: ${r.key}, Value: ${r.value}")
      logger.info(s"Partition: ${r.partition}, Offset: ${r.offset}")
    })
  }


}
