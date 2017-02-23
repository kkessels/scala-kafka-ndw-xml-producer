package nl.trivento.fastdata.api

import java.io.InputStream

import scala.collection.mutable
import scala.io.Source
import scala.xml._
import scala.xml.pull._

/**
  * Created by koen on 13/02/2017.
  */
object XmlIngestor {
  def fromInputStream(inputStream: InputStream, triggers: Map[String, (Elem) => Option[Elem]]): Unit = {
    val reader: XMLEventReader = new XMLEventReader(Source.fromInputStream (inputStream) (scala.io.Codec.fallbackSystemCodec) )
    var path = "/"
    val stack = new mutable.Stack[Elem]

    def start(prefix: String, label: String, attributes: MetaData, scope: NamespaceBinding) {
      path = path + (if (prefix != null) prefix + ":" + label + "/" else label + "/")
      stack.push(Elem(prefix, label, attributes, scope, false))
    }

    def end(name: String): Unit = {
      if (!path.endsWith(name + "/")) throw new IllegalStateException("Bad XML")

      val done = stack.pop()
      if (stack.nonEmpty) {
        triggers
          .getOrElse(path, (e: Elem) => Option(e))(done)
          .foreach(e => stack.update(0, stack.top.copy(child = stack.top.child :+ e)))
      }

      path = path.substring(0, path.length - name.length - 1)
    }

    while (reader.hasNext) {
      reader.next() match {
        case EvText(text) =>
          stack.update(0, stack.top.copy(child = stack.top.child :+ Text(text)))
        case EvElemStart(null, label: String, attrs: MetaData, scope: NamespaceBinding) =>
          start(null, label, attrs, scope)
        case EvElemStart(pre: String, label: String, attrs: MetaData, scope: NamespaceBinding) =>
          start(pre, label, attrs, scope)
        case EvElemEnd(null, label: String) =>
          end(label)
        case EvElemEnd(pre: String, label: String) =>
          end(pre + ":" + label)
        case EvEntityRef(entity) =>
          stack.update(0, stack.top.copy(child = stack.top.child :+ EntityRef(entity)))
      }
    }
  }

}
