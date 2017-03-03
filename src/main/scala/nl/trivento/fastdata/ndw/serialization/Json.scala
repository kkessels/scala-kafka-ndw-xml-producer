package nl.trivento.fastdata.ndw.serialization

import com.fasterxml.jackson.annotation.JsonInclude.Include
import com.fasterxml.jackson.annotation.JsonTypeInfo
import com.fasterxml.jackson.databind.{JavaType, ObjectMapper}
import com.fasterxml.jackson.databind.ObjectMapper.DefaultTypeResolverBuilder
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.kafka.common.serialization.{Deserializer, Serializer}
import java.util.Map

/**
  * Created by koen on 27/02/2017.
  */
class JsonSerializer[T](cls: Class[T]) extends Serializer[T] {
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