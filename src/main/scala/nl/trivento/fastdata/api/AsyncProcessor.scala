package nl.trivento.fastdata.api

import java.util.{Properties, UUID}

import akka.Done
import akka.actor.ActorSystem
import akka.kafka._
import akka.kafka.scaladsl.Consumer
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Sink
import org.apache.kafka.clients.producer.{ProducerRecord, RecordMetadata}
import org.apache.kafka.common.serialization.{Deserializer, Serializer}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

/**
  * Created by koen on 07/02/2017.
  */
abstract class AsyncProcessor[INKEY, INTYPE, OUTKEY, OUTTYPE](system: ActorSystem,
                                                              sourceTopicName: String,
                                                              keydeserializer: Deserializer[INKEY],
                                                              valueDeserializer: Deserializer[INTYPE],
                                                              destinationTopicName: String,
                                                              keySerializer: Serializer[OUTKEY],
                                                              valueSerializer: Serializer[OUTTYPE]) {
  private implicit val materializer = ActorMaterializer.create(system)

//  private val consumerProperties = new Properties()
//  consumerProperties.put("group.id", "requestConsumer")
//  consumerProperties.put("zookeeper.connect", "localhost:2181")
//  consumerProperties.put("auto.offset.reset", "smallest")
//  consumerProperties.put("consumer.timeout.ms", "120000")
//  consumerProperties.put("auto.commit.interval.ms", "10000")
//  private val consumerSettings = ConsumerSettings.create(new ConsumerConfig(consumerProperties).asInstanceOf[Config], keydeserializer, valueDeserializer)

  private val consumerSettings = ConsumerSettings[INKEY, INTYPE](system, keydeserializer, valueDeserializer)
    .withBootstrapServers("localhost:9092")
    .withClientId(UUID.randomUUID().toString)
    .withGroupId("requestConsumerGroup")
    .withProperty("zookeeper.connect", "localhost:2181")
    .withProperty("auto.offset.reset", "earliest")
    .withProperty("consumer.timeout.ms", "120000")
    .withProperty("auto.commit.interval.ms", "10000")

  private val producerProperties = new Properties()
    producerProperties.put("producer.type", "sync")
    producerProperties.put("metadata.broker.list", "localhost:9092")
    producerProperties.put("bootstrap.servers", "localhost:9092")
    producerProperties.put("message.send.max.retries", "5")
    producerProperties.put("request.required.acks", "-1")
    producerProperties.put("serializer.class", "kafka.serializer.StringEncoder")
    producerProperties.put("client.id", UUID.randomUUID().toString)


  def run(): Unit = {
    Consumer.committableSource(consumerSettings, Subscriptions.topics(sourceTopicName))
      .mapAsync(32){ msg => asMessages(msg).sendAll(new ScalaKafkaProducer[OUTKEY, OUTTYPE](producerProperties, keySerializer, valueSerializer)).ack() }
      .runWith(Sink.ignore)
  }

  private case class SentMessages(offset: ConsumerMessage.CommittableOffset, results: Future[Traversable[Future[RecordMetadata]]]){
    def ack(): Future[Future[Done]] = results.map(f => Future.sequence(f)).map(f => offset.commitScaladsl())
  }

  private case class Messages(offset: ConsumerMessage.CommittableOffset, items: Future[Traversable[ProducerRecord[OUTKEY, OUTTYPE]]]) {
    def sendAll(kafkaProducer: ScalaKafkaProducer[OUTKEY, OUTTYPE]): SentMessages =
      SentMessages(offset, items.map(records => records.map(kafkaProducer.send)))
  }

  private def asMessages(input: ConsumerMessage.CommittableMessage[INKEY, INTYPE]): Messages =
    Messages(input.committableOffset, action(input.record.key(), input.record.value()))

  def action(key: INKEY, value: INTYPE): Future[Traversable[ProducerRecord[OUTKEY, OUTTYPE]]]
}
