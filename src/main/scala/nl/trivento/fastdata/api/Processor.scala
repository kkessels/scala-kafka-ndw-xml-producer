package nl.trivento.fastdata.api

import java.util.{Properties, UUID}
import java.util.concurrent.{Future, TimeUnit}

import akka.actor.ActorSystem
import akka.kafka.scaladsl.Consumer
import akka.kafka.{ConsumerMessage, ConsumerSettings, ProducerSettings, Subscriptions}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Sink
import kafka.message.SnappyCompressionCodec
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.serialization.{Deserializer, Serializer}

import scala.concurrent.duration.FiniteDuration

/**
  * Created by koen on 08/02/2017.
  */
abstract class Processor[INKEY, INTYPE, OUTKEY, OUTTYPE](system: ActorSystem,
                                                         sourceTopicName: String,
                                                         keydeserializer: Deserializer[INKEY],
                                                         valueDeserializer: Deserializer[INTYPE],
                                                         destinationTopicName: String,
                                                         keySerializer: Serializer[OUTKEY],
                                                         valueSerializer: Serializer[OUTTYPE]) {
  private implicit val materializer = ActorMaterializer.create(system)

  private val consumerProperties = Map[String, String](
    "compression.codec" -> SnappyCompressionCodec.codec.toString,
    "producer.type" -> "sync",
    "metadata.broker.list" -> "master:9092",
    "bootstrap.servers" -> "master:9092",
    "message.send.max.retries" -> "5",
    "request.required.acks" -> "-1",
    "client.id" -> UUID.randomUUID().toString)

  private val consumerSettings = new ConsumerSettings[INKEY, INTYPE](consumerProperties, Some(keydeserializer),
    Some(valueDeserializer),
    FiniteDuration(10, TimeUnit.MILLISECONDS),
    FiniteDuration(10000, TimeUnit.MILLISECONDS),
    FiniteDuration(10000, TimeUnit.MILLISECONDS),
    FiniteDuration(10000, TimeUnit.MILLISECONDS),
    FiniteDuration(10000, TimeUnit.MILLISECONDS),
    FiniteDuration(10000, TimeUnit.MILLISECONDS),
    10,
    null)

  private val producerProperties = Map(
    "compression.codec" -> SnappyCompressionCodec.codec.toString,
    "producer.type" -> "sync",
    "metadata.broker.list" -> "master:9092",
    "bootstrap.servers" -> "master:9092",
    "message.send.max.retries" -> "5",
    "request.required.acks" -> "-1",
    "client.id" -> UUID.randomUUID().toString)

  private val producerSettings = new ProducerSettings[OUTKEY, OUTTYPE](producerProperties, Some(keySerializer), Some(valueSerializer), FiniteDuration(10000, TimeUnit.MILLISECONDS), 1, null)

  def run(): Unit =
    Consumer.committableSource(consumerSettings, Subscriptions.topics(sourceTopicName))
      .map(in => asMessages(in))
      .map(msgs => msgs.sendAll(producerSettings.createKafkaProducer).ack())
      .runWith(Sink.ignore)

  private case class SentMessages(offset: ConsumerMessage.CommittableOffset, results: Seq[Future[RecordMetadata]]) {
    def ack(): Unit = offset.commitScaladsl()
  }

  private case class Messages(offset: ConsumerMessage.CommittableOffset, items: Seq[ProducerRecord[OUTKEY, OUTTYPE]]) {
    def sendAll(kafkaProducer: KafkaProducer[OUTKEY, OUTTYPE]): SentMessages =
      SentMessages(offset, items.map(item => kafkaProducer.send(item)))
  }

  private def asMessages(input: ConsumerMessage.CommittableMessage[INKEY, INTYPE]): Messages =
    Messages(input.committableOffset, action(input.record.key(), input.record.value()))

  def action(key: INKEY, value: INTYPE): Seq[ProducerRecord[OUTKEY, OUTTYPE]]
}