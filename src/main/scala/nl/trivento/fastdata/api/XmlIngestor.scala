package nl.trivento.fastdata.api

import java.io.InputStream

import scala.io.Source
import scala.xml._
import scala.xml.pull._

/**
  * Created by koen on 13/02/2017.
  */
object XmlIngestor {
  def apply(triggers: Map[String, (Elem) => Option[Elem]]): XmlIngestor = {
    new XmlIngestor(triggers)
  }
}

class XmlIngestor(triggers: Map[String, (Elem) => Option[Elem]]) {
  def fromInputStream(inputStream: InputStream): Unit = {
    val reader: XMLEventReader = new XMLEventReader(Source.fromInputStream(inputStream)(scala.io.Codec.fallbackSystemCodec))
    var path = "/"
    var stack = Seq.empty[Elem]

    def start(prefix: String, label: String, attributes: MetaData, scope: NamespaceBinding) {
      path = path + (if (prefix != null) prefix + ":" + label + "/" else label + "/")
      stack = Elem(prefix, label, attributes, scope, false) +: stack
    }

    def end(name: String): Unit = {
      if (!path.endsWith(name + "/")) throw new IllegalStateException("Bad XML")

      stack = stack match {
        case Seq(done, tail @ _*) =>
          triggers.get(path).map(f => f(done)).getOrElse(Option(done)).map(updateStack(tail, _)).getOrElse(tail)
        case _ => Seq.empty
      }

      path = path.substring(0, path.length - name.length - 1)
    }

    def updateStack(stack: Seq[Elem], child: Node): Seq[Elem] = {
      stack match {
        case Seq(parent, tail @ _*) => updateElem(parent, child) +: tail
        case Seq(parent) => Seq(parent)
        case Nil => Nil
      }
    }

    def updateElem(elem: Elem, child: Node): Elem = {
      elem.copy(child = elem.child :+ child)
    }

    while (reader.hasNext) {
      reader.next() match {
        case EvText(text) =>
          stack = updateStack(stack, Text(text))
        case EvElemStart(null, label: String, attrs: MetaData, scope: NamespaceBinding) =>
          start(null, label, attrs, scope)
        case EvElemStart(pre: String, label: String, attrs: MetaData, scope: NamespaceBinding) =>
          start(pre, label, attrs, scope)
        case EvElemEnd(null, label: String) =>
          end(label)
        case EvElemEnd(pre: String, label: String) =>
          end(pre + ":" + label)
        case EvEntityRef(entity) =>
          updateStack(stack, EntityRef(entity))
      }
    }
  }

}
