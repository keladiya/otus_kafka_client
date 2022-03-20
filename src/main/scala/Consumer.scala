import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer

import scala.jdk.CollectionConverters._
import java.util.Properties
import java.time.Duration

object Consumer extends App {

  val topicName = "books"
  val partCount = 3
  val tailSize = 5
  val bufferSize = 15

  val props = new Properties()

  props.put("bootstrap.servers", "localhost:29092")
  props.put("max.poll.records", bufferSize)
  props.put("group.id", "consumer_all")

  val consumer = new KafkaConsumer( props, new StringDeserializer, new StringDeserializer)

  val topicParts = for (i <- 0 until partCount) yield new TopicPartition( topicName, i)

  consumer.assign( topicParts.asJava )

  try {
    var countRead = 0
    var continue = true

    while (continue) {
      val recs = consumer.poll( Duration.ofSeconds(1) ).asScala
      countRead += recs.size
      continue = recs.nonEmpty
    }

    println(s"Последние $tailSize записей из каждой партиции:")

    consumer.seekToEnd( topicParts.asJava )

    for (part <- topicParts) {
      consumer.seek( part, consumer.position( part ) - tailSize)
      val recs = consumer.poll( Duration.ofSeconds(1) ).asScala
      recs.foreach { r =>  println(s"${r.partition()}:${r.offset()}   ${r.value()}") }
    }

  }
  finally {
    consumer.close()
  }
}