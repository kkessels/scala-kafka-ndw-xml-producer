package nl.trivento.fastdata.ndw.processor

import akka.actor.{Actor, ActorRef, ActorSystem, Props, Terminated}
import akka.http.scaladsl.model.ws.TextMessage
import akka.stream.ActorMaterializer
import nl.trivento.fastdata.ndw.presentation.AddSubscription

/**
  * Created by koen on 07/04/2017.
  */
object Dispatch {
  def apply(implicit system: ActorSystem, materializer: ActorMaterializer) {
    val dispatchActor = system.actorOf(
      Props(
        new Actor {
          var subscriptions = Set.empty[ActorRef]

          override def receive: Receive = {
            case AddSubscription =>
              context.watch(sender)
              subscriptions += sender
            case Terminated =>
              subscriptions -= sender
            case msg: TextMessage =>
              subscriptions.foreach(_ ! msg)
          }
        }
      )
    )
  }
}

