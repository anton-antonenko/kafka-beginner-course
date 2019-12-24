package beginner.tutorial1

import java.util.Properties

import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerConfig, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.LoggerFactory

object ProducerWithKeyDemo extends App {

  val logger = LoggerFactory.getLogger(this.getClass.getSimpleName)

  // set up configs
  val props = new Properties()
  props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
  props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)


  // create a Producer
  val producer = new KafkaProducer[String, String](props)

  // send data

  val topic = "second"
  val callback = new Callback {
    override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
      if (exception == null) {
        logger.info(s"Received new metadata:\n" +
          s"Topic:      ${metadata.topic}\n" +
          s"Partition:  ${metadata.partition}\n" +
          s"Offset:     ${metadata.offset}\n" +
          s"Timestamp:  ${metadata.timestamp}\n")
      } else
        logger.error("Error while producing!!!!", exception)
    }
  }

  0 to 10 foreach(i => {
    val key = s"id_$i"
    val record = new ProducerRecord[String, String](topic, key, s"value from code $i")
    logger.info(s"Key: $key")
    producer.send(record, callback).get() // get is blocking and needed only for tests
  })

  producer.flush()
  producer.close()
}
