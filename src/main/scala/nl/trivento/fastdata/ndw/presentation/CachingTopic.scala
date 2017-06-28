package nl.trivento.fastdata.ndw.presentation

import akka.actor.{Actor, ActorLogging, ActorRef, Terminated}

import scala.collection.mutable

abstract class CachingTopic[IN, K](getKey: (IN) => K, getValue: (IN) => String) extends Actor with ActorLogging {
  private val cache = mutable.HashMap.empty[K, String]
  protected val subscriptions = mutable.HashSet.empty[ActorRef]

  override def preStart(): Unit = {
    super.preStart()
  }

  override def receive: Receive = {
    case AddSubscription =>
      context.watch(sender())
      log.info("New subscription, send " + cache.values.size + " heat points in cache")
      sender ! cache.values.toArray[String]
      subscriptions.add(sender())
    case RemoveSubscription =>
      subscriptions.remove(sender())
    case m: IN =>
      val value = getValue(m)
      val key = getKey(m)
      cache.put(key, value)
      notify(key, value)
    case Terminated =>
      subscriptions.remove(sender())
  }

  def notify(key: K, value: String)
}

class BufferingCachingTopic[IN, K](getKey: (IN) => K, getValue: (IN) => String, interval: Int, maxUpdates: Int)
    extends CachingTopic[IN, K](getKey, getValue) {
  private var last: Long = 0
  private var updates = 0
  private val cache = mutable.HashMap.empty[K, String]

  def notify(key: K, value: String): Unit = {
    updates += 1
    cache.put(key, value)
    if (System.currentTimeMillis() - last >= interval || updates >= maxUpdates) {
      val aggregatedUpdate = cache.values.toArray[String]
      subscriptions.foreach(_ ! aggregatedUpdate)
      cache.clear()
      last = System.currentTimeMillis()
      updates = 0
    }
  }
}

case class AddSubscription()

case class Subscribe(listener: ActorRef, topic: ActorRef)

case class RemoveSubscription()

class Subscription[V] extends Actor with ActorLogging {
  private var listener: Option[ActorRef] = None

  override def receive: Receive = {
    case s: Subscribe =>
      context.become(subscription)
      listener = Option(s.listener)
      s.topic ! AddSubscription
  }

  def subscription: Receive = {
    case values: Array[V] =>
      listener.foreach(_ ! values)
  }
}