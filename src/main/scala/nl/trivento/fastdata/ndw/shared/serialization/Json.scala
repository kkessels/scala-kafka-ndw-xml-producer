package nl.trivento.fastdata.ndw.shared.serialization

import com.fasterxml.jackson.annotation.JsonInclude.Include
import com.fasterxml.jackson.annotation.JsonTypeInfo
import com.fasterxml.jackson.databind.{JavaType, ObjectMapper}
import com.fasterxml.jackson.databind.ObjectMapper.DefaultTypeResolverBuilder
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.kafka.common.serialization.{Deserializer, Serde, Serializer}
import java.util.Map

import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import nl.trivento.fastdata.ndw.MeasurementExtra

/**
  * Created by koen on 27/02/2017.
  */
class TypedJsonSerializer[T](cls: Class[T]) extends Serializer[T] {
  private val mapper = new ObjectMapper()

  mapper.setDefaultTyping(new DefaultTypeResolverBuilder(null) {
    init(JsonTypeInfo.Id.CLASS, null)
    inclusion(JsonTypeInfo.As.PROPERTY)

    override def useForType(t: JavaType): Boolean =
      !t.isPrimitive && t.getRawClass.getPackage.getName == "nu.ndw"
  })
  mapper.setSerializationInclusion(Include.NON_EMPTY)
  mapper.registerModule(DefaultScalaModule)

  override def configure(configs: Map[String, _], isKey: Boolean): Unit = {

  }

  override def serialize(topic: String, data: T): Array[Byte] = mapper.writeValueAsBytes(data)

  override def close(): Unit = {

  }
}

class TypedJsonDeserializer[T](cls: Class[T]) extends Deserializer[T] {
  private val mapper = new ObjectMapper()
  mapper.setDefaultTyping(new DefaultTypeResolverBuilder(null) {
    init(JsonTypeInfo.Id.CLASS, null)
    inclusion(JsonTypeInfo.As.PROPERTY)

    override def useForType(t: JavaType): Boolean =
      !t.isPrimitive && t.getRawClass.getPackage.getName == "nu.ndw" && !t.isFinal
  })
  mapper.setSerializationInclusion(Include.NON_EMPTY)
  mapper.registerModule(DefaultScalaModule)

  override def configure(configs: Map[String, _], isKey: Boolean): Unit = {

  }

  override def deserialize(topic: String, data: Array[Byte]): T = mapper.readValue(data, cls)

  override def close(): Unit = {

  }
}

class AnyJsonDeserializer extends Deserializer[Object] {
  private val mapper = new ObjectMapper()
  mapper.registerModule(DefaultScalaModule)

  override def configure(configs: Map[String, _], isKey: Boolean): Unit = {

  }

  override def deserialize(topic: String, data: Array[Byte]): Object = mapper.readValue(data, classOf[Object])

  override def close(): Unit = {

  }
}

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