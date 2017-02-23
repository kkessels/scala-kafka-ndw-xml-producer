package nl.trivento.fastdata.api

import akka.actor.ActorSystem
import akka.kafka.ProducerMessage.{Message, Result}
import akka.kafka._
import akka.kafka.scaladsl.{Consumer, Producer}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Flow, Sink}
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.serialization.{Deserializer, Serializer}
import java.util.concurrent.Future

import akka.Done

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

  private val consumerSettings = ConsumerSettings[INKEY, INTYPE](system, keydeserializer, valueDeserializer)
    .withBootstrapServers("127.0.1.1:9092")
    .withGroupId("requestConsumerGroup")

  private val producerSettings = ProducerSettings[OUTKEY, OUTTYPE](system, keySerializer, valueSerializer)
    .withBootstrapServers("127.0.0.1:9092")

  def run(): Unit =
    Consumer.committableSource(consumerSettings, Subscriptions.topics(sourceTopicName))
      .map(in => asMessages(in))
      .map(msgs => msgs.sendAll(producerSettings.createKafkaProducer).ack())
      .runWith(Sink.ignore)

  private case class SentMessages(offset: ConsumerMessage.CommittableOffset, results: Traversable[Future[RecordMetadata]]) {
    def ack(): Unit = offset.commitScaladsl()
  }

  private case class Messages(offset: ConsumerMessage.CommittableOffset, items: Traversable[ProducerRecord[OUTKEY, OUTTYPE]]) {
    def sendAll(kafkaProducer: KafkaProducer[OUTKEY, OUTTYPE]): SentMessages =
      SentMessages(offset, items.map(item => kafkaProducer.send(item)))
  }

  private def asMessages(input: ConsumerMessage.CommittableMessage[INKEY, INTYPE]): Messages =
    Messages(input.committableOffset, action(input.record.key(), input.record.value()))

  def action(key: INKEY, value: INTYPE): Traversable[ProducerRecord[OUTKEY, OUTTYPE]]
}