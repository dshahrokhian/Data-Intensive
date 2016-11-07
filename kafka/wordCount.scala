import java.util.HashMap

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import kafka.serializer.{DefaultDecoder, StringDecoder}

import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.storage.StorageLevel

object KafkaWordCount {
  def main(args: Array[String]) {

    val kafkaConf = Map(
	"metadata.broker.list" -> "localhost:9092",
	"zookeeper.connect" -> "localhost:2181",
	"group.id" -> "kafka-spark-streaming",
	"zookeeper.connection.timeout.ms" -> "1000")

    val sparkConf = new SparkConf().setAppName("KafkaWordCount").setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(2))
    ssc.checkpoint("checkpoint")

    // If you want to try the receiver-less approach, uncomment line below and comment the next one
    //val messages = KafkaUtils.createStream[String, String, DefaultDecoder, StringDecoder](<FILL IN>)
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaConf, Set("avg"))

    val values = messages.map(_._2)
    val pairs = values.map(_.split(",") match { case Array(letter, value) => (letter, value.toInt)})


    def mappingFunc(key: String, value: Option[Int], state: State[(Double, Long)]): Option[(String, Double)] = {
        
        val (prevAvg, prevN) = state.getOption.getOrElse((0D, 0L))
        val n = prevN + 1
        val avg = (value.getOrElse(0).toDouble + prevAvg * prevN) / n
        state.update((avg, n))
        
        return Some((key, avg))
    }

    val stateDstream = pairs.mapWithState(StateSpec.function(mappingFunc _))

    stateDstream.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
