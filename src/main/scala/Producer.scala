import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import java.util.Properties

import org.apache.commons.csv.{CSVFormat, CSVParser, CSVRecord}
import java.nio.charset.Charset
import java.io.File
import scala.jdk.CollectionConverters._
import io.circe.generic.auto._


object Producer extends App {

  case class Book(
   name: String
   , author: String
   , userRating: Double
   , reviews: Int
   , price: Double
   , year: Int
   , genre: String
 )

  val list = BookCSVParser.parse( new File("./src/main/resources/bestsellers_with_categories.csv"),
    r => Book(
      r.get("Name")
      , r.get("Author")
      , r.get("User Rating").toDouble
      , r.get("Reviews").toInt
      , r.get("Price").toDouble
      , r.get("Year").toInt
      , r.get("Genre")
    )
  )
  val jsonBooks = for (book <- list) yield io.circe.Encoder[ Book ].apply( book ).noSpaces

  val props = new Properties()

  props.put("bootstrap.servers", "localhost:29092")

  val producer = new KafkaProducer( props, new StringSerializer, new StringSerializer)

  jsonBooks.foreach {m => producer.send( new ProducerRecord("books", m, m))}

  producer.close()
}

object BookCSVParser {
  def parse[ A ](file: File, mapDefinition: CSVRecord => A): List[ A ] = {
    val parser = CSVParser.parse( file, Charset.forName("UTF-8"), CSVFormat.DEFAULT.withHeader())
    parser.getRecords.asScala.toList.map( mapDefinition )
  }
}