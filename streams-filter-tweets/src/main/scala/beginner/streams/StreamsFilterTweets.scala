package beginner.streams

import java.util.Properties

import com.google.gson.JsonParser
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.{KafkaStreams, StreamsBuilder, StreamsConfig}
import org.apache.kafka.streams.kstream.KStream

import scala.util.Try

object StreamsFilterTweets extends App {

  // create properties
  val props = new Properties()
  props.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  props.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "demo_kafka_streams")
  props.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, classOf[Serdes.StringSerde].getName)
  props.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, classOf[Serdes.StringSerde].getName)

  // write transformations
  val builder = new StreamsBuilder
  val inputTopic: KStream[String, String] = builder.stream("twitter_tweets")
  val filteredStream: KStream[String, String] = inputTopic.filter((_, tweet) => extractFollowersCountFromTweetJson(tweet) > 10000)
  filteredStream.to("important_tweets")

  // build a topology
  val kafkaStreams = new KafkaStreams(builder.build(), props)

  // start app
  kafkaStreams.start()




  def extractFollowersCountFromTweetJson(json: String): Int = {
    Try {
      JsonParser.parseString(json)
        .getAsJsonObject
        .get("user").getAsJsonObject
        .get("followersCount").getAsInt
    } getOrElse 0
  }

}
