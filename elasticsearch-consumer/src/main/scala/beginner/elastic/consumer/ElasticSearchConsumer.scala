package beginner.elastic.consumer

import java.util.Properties
import java.util.concurrent.TimeUnit

import com.google.gson.JsonParser
import org.apache.http.HttpHost
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.serialization.StringDeserializer
import org.elasticsearch.action.bulk.BulkRequest
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.{RequestOptions, RestClient, RestHighLevelClient}
import org.elasticsearch.common.xcontent.XContentType
import org.slf4j.LoggerFactory

import scala.concurrent.duration._
import scala.jdk.CollectionConverters._
import scala.jdk.DurationConverters._

object ElasticSearchConsumer extends App {
  val logger = LoggerFactory.getLogger(this.getClass.getSimpleName)

  val client = createClient()
  val consumer = createConsumer("twitter_tweets")

  // consume records
  while (true) {
    val records = consumer.poll(200.millis.toJava)

    if (records.count() > 0) {
      logger.info(s"Received ${records.count()} records")
      val bulk = new BulkRequest
      records.forEach(r => {
        // To make consumer idempotent we need to use some uniq IDs for tweets
        // The generic one could be the following:
        // val id = s"${r.topic()}_${r.partition()}_${r.offset()}"

        // but it's better to use Twitter specific ID:
        val id = extractIdFromTweetJson(r.value())
        val req = new IndexRequest("twitter", "tweets", id)
          .source(r.value(), XContentType.JSON)

        bulk.add(req)
      })
      val responses = client.bulk(bulk, RequestOptions.DEFAULT)
      //responses.asScala.foreach(resp => logger.info(s"Document created id: ${resp.getId}"))

      logger.info("Committing offsets...")
      consumer.commitSync()
      logger.info("Offsets committed")
    }
    TimeUnit.SECONDS.sleep(1)
  }


  // close elastic client and consumer
  client.close()
  consumer.close()




  // service methods

  private def createConsumer(topic: String): KafkaConsumer[String, String] = {
    val groupId = "kafka-demo-elasticsearch"
    // set up configs
    val props = new Properties()
    props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId)
    props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false") // we want to commit offsets manually
    props.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100")

    // create a consumer
    val consumer = new KafkaConsumer[String, String](props)
    consumer.subscribe(Seq(topic).asJava)
    consumer
  }


  def createClient(): RestHighLevelClient = {
    val host = "127.0.0.1"
    val builder = RestClient.builder(new HttpHost(host, 9200, "http"))
    new RestHighLevelClient(builder)
  }

  def extractIdFromTweetJson(json: String): String = {
    JsonParser.parseString(json).getAsJsonObject.get("id").getAsString
  }
}
