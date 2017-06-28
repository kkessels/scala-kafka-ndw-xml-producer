package nl.trivento.fastdata.ndw.shared.serialization

import java.util.Map

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import nl.trivento.fastdata.ndw.processing.SparkStreaming.MeasurementExtra
import org.apache.kafka.common.serialization.{Deserializer, Serde, Serializer}

abstract class TypedJsonSerde[T] extends Serializer[T] with Deserializer[T] with Serde[T] {

  override def configure(configs: Map[String, _], isKey: Boolean): Unit = {}

  override def close(): Unit = {}
}

class MeasurementExtraJsonSerde extends TypedJsonSerde[MeasurementExtra] {

  private val mapper = new ObjectMapper() with ScalaObjectMapper

  mapper.enableDefaultTyping()
//  mapper.setSerializationInclusion(Include.NON_EMPTY)
  mapper.registerModule(DefaultScalaModule)

  override def serialize(topic: String, data: MeasurementExtra): Array[Byte] = mapper.writeValueAsBytes(data)

  override def deserialize(topic: String, data: Array[Byte]): MeasurementExtra = mapper.readValue[MeasurementExtra](data)

  override def serializer() = new MeasurementExtraJsonSerde
  override def deserializer() = new MeasurementExtraJsonSerde
}