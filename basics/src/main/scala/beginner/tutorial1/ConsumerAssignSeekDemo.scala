package beginner.tutorial1

import java.util.Properties

import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.TopicPartition
import org.slf4j.LoggerFactory

import scala.language.postfixOps

object ConsumerAssignSeekDemo extends App {

  val logger = LoggerFactory.getLogger(this.getClass.getSimpleName)

  // set up configs
  val props = new Properties()
  props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
  props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
  props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")


  // create a consumer
  val consumer = new KafkaConsumer[String, String](props)

  // assign and seek: reeds messages from specific topic-partition
  import scala.concurrent.duration._
  import scala.jdk.CollectionConverters._
  import scala.jdk.DurationConverters._

  val topic = "second"
  val partition = new TopicPartition(topic, 1)
  consumer.assign(Seq(partition).asJava)

  consumer.seek(partition, 5)

  val recordsToRead = 5

  // consume records
  var recordsRead = 0
  while (recordsToRead > recordsRead) {
    val records = consumer.poll(100.millis.toJava)

    records.iterator().asScala.take(recordsToRead - recordsRead).foreach(r => {
      logger.info(s"Key: ${r.key}, Value: ${r.value}")
      logger.info(s"Partition: ${r.partition}, Offset: ${r.offset}")
      recordsRead += 1
    })
  }

  logger.info("Exiting")


}
