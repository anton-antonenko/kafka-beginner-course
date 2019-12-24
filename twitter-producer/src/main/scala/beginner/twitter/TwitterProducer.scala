package beginner.twitter

import java.util.concurrent.{ArrayBlockingQueue, BlockingQueue}
import java.util.concurrent.atomic.AtomicBoolean
import java.util.Properties

import com.google.gson.GsonBuilder
import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerConfig, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.LoggerFactory
import twitter4j.{ConnectionLifeCycleListener, FilterQuery, Status, TwitterStream, TwitterStreamFactory}

import scala.util.control.NonFatal

object TwitterProducer extends App {

  val logger = LoggerFactory.getLogger(this.getClass.getName)
  val gson = new GsonBuilder().create()

  // create a Twitter Client
  val isStreamOpened = new AtomicBoolean(true)
  val messageQueue = new ArrayBlockingQueue[Status](1000)
  val twitterStream = twitter(messageQueue, isStreamOpened)
    .filter(new FilterQuery()
      //.language("en")
      .track("kafka", "scala", "apache spark", "big data", "bigdata",
        "blockchain", "java", "python",
        "trump", "warming", "impeachment"
      )
    )


  // Create a Kafka Producer
  val producer = kafkaProducer

  val callback = new Callback {
    override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
      if (exception != null) logger.error("Error on put data to Kafka", exception)
    }
  }

  Runtime.getRuntime.addShutdownHook(new Thread(() => {
    logger.info("Shutting down...")
    twitterStream.shutdown()
    producer.close()
    logger.info("Exiting.")
  }))


  // send tweets to Kafka
  val topic = "twitter_tweets"

  while (isStreamOpened.get()) {
    try {
      val tweet = messageQueue.take()
      val jsonTweet = gson.toJson(tweet)
      logger.info(s"New message from ${tweet.getUser.getName}:\n$jsonTweet")

      producer.send(new ProducerRecord[String, String](topic, jsonTweet), callback)
    } catch {
      case NonFatal(e) =>
        logger.error("Error polling a message", e)
        twitterStream.shutdown()
    }
  }
  logger.info("Producer closed. Exiting.")






  // Service methods

  def twitter(queue: BlockingQueue[Status], isStreamOpened: AtomicBoolean): TwitterStream = {
    import twitter4j.conf.ConfigurationBuilder

    val cb = new ConfigurationBuilder()
    cb.setDebugEnabled(true)
      .setOAuthConsumerKey(System.getenv("ConsumerKey"))
      .setOAuthConsumerSecret(System.getenv("ConsumerSecret"))
      .setOAuthAccessToken(System.getenv("AccessToken"))
      .setOAuthAccessTokenSecret(System.getenv("AccessTokenSecret"))
    val tf = new TwitterStreamFactory(cb.build())
    val twitter = tf.getInstance()
      .onStatus(status => queue.put(status))
      .onException(ex => logger.error("Got Twitter exception", ex))
      .addConnectionLifeCycleListener(new ConnectionLifeCycleListener {
        override def onConnect(): Unit = {
          logger.info("Twitter Stream opened")
          isStreamOpened.set(true)
        }
        override def onDisconnect(): Unit = {
          logger.info("Twitter Stream closed")
          isStreamOpened.set(false)
        }
        override def onCleanUp(): Unit = {
          logger.info("Twitter client cleanup")
          isStreamOpened.set(false)}
      })
    twitter
  }

  lazy val kafkaProducer: KafkaProducer[String, String] = {
    val props = new Properties()
    props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    // set this properties to make producer idempotent and safe
    props.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")
    props.setProperty(ProducerConfig.ACKS_CONFIG, "all")
    props.setProperty(ProducerConfig.RETRIES_CONFIG, Int.MaxValue.toString)
    props.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5")
    // and some compression
    props.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy") // or "lz4", or "gzip"
    props.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, (32*1024).toString) // max batch size in bytes
    props.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20") // collects batch for this duration
    // ADVANCED!!!
    props.setProperty(ProducerConfig.BUFFER_MEMORY_CONFIG, (32*1024*1024).toString) // the size of incoming message buffer. If it's full producer blocks on .send()
    props.setProperty(ProducerConfig.MAX_BLOCK_MS_CONFIG, "60000") // duration for which .send() will be blocked

    new KafkaProducer[String, String](props)
  }
}


