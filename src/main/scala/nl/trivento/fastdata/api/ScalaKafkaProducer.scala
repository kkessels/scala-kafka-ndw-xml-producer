package nl.trivento.fastdata.api

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.serialization.Serializer
import org.apache.kafka.common.{Metric, MetricName, PartitionInfo}

import scala.collection.JavaConversions._
import scala.concurrent._
import scala.concurrent.duration.TimeUnit
import scala.util.Try

/**
  * Created by koen on 22/02/2017.
  */
class ScalaKafkaProducer[K, V](properties: Properties, keySerializer: Serializer[K], valueSerializer: Serializer[V]) {
  val kafkaProducer = new KafkaProducer[K, V](properties, keySerializer, valueSerializer)

  def send(producerRecord: ProducerRecord[K, V]): Future[RecordMetadata] = {
    val promise = Promise[RecordMetadata]
    kafkaProducer.send(
      producerRecord,
      (metadata: RecordMetadata, exception: Exception) =>
        if (exception != null)
          promise.failure(exception)
        else
          promise.success(metadata))
    promise.future
  }

  def close(): Try[Unit] = Try { kafkaProducer.close() }
  def close(timeout: Long, unit: TimeUnit): Try[Unit] = Try { kafkaProducer.close(timeout, unit) }
  def flush(): Try[Unit] = Try { kafkaProducer.flush() }
  def metrics: Map[MetricName, _ <: Metric] = kafkaProducer.metrics().toMap
  def partitionsFor(topic: String): List[PartitionInfo] = kafkaProducer.partitionsFor(topic).toList
}
