/*
 * stephane.nsakala@luxairgroup.lu - 2014
 */

package edifactdbloader.concurrent

import edifactdbloader.parser.EDIMsg
import scala.xml.XML
import scala.io.Source
import akka.actor.Actor
import akka.actor.ActorSystem
import akka.actor.PoisonPill
import akka.actor.Props
import akka.actor.Terminated
import akka.routing.Broadcast
import akka.routing.RoundRobinRouter

import edifactdbloader.parser.EDIParser
import edifactdbloader.parser.EDIMsgIterator
import edifactdbloader.parser.EDIRawSegIterator

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

import EDIActor._

class ParserActor(parser: EDIParser) extends Actor {
  def receive = {
    case m: EDIMsg => println(parser.parseMsg(m)) //each thread should write in separate file
  }
}

object ParserActor {
  def props(parser: EDIParser, instNbr: Int): Props = Props(new ParserActor(parser)).withRouter(RoundRobinRouter(nrOfInstances = instNbr))
}

class ProducerActor(msgIt: EDIMsgIterator) extends Actor {
  def receive = {
    case "produce" => {
      val consumer = context.actorSelection("../consumer")
      do {
        consumer ! msgIt.next
      } while (msgIt.hasNext && msgIt.msgNbr % maxMsg != 0)
      if (msgIt.hasNext) self ! "throttle" else self ! "shutdownWorkers"
    }
    case "throttle" => context.system.scheduler.scheduleOnce(timer seconds, self, "produce")
    case "shutdownWorkers" => {
      val consumer = context.actorSelection("../consumer")
      System.err.println("No more msg, shutdown workers...")
      consumer ! Broadcast(PoisonPill)
    }
  }
}

object ProducerActor {
  def props(msgIt: EDIMsgIterator): Props = Props(new ProducerActor(msgIt))
}

class ConcurrentParser(ruleFilename: String) extends Actor {
  val parser = new EDIParser()
  parser.ruleFromXML(XML.loadFile(ruleFilename))
  val msgIt = new EDIMsgIterator(new EDIRawSegIterator(Source.stdin))

  val consumer = context.actorOf(ParserActor.props(parser, workers), name = "consumer")
  context.watch(consumer)
  val producer = context.actorOf(ProducerActor.props(msgIt), name = "producer")

  def receive = {
    case "start"             => producer ! "produce"
    case Terminated(watched) => context.system.shutdown
  }
}

object ConcurrentParser {
  def props(filename: String): Props = Props(new ConcurrentParser(filename))
}

object EDIActor {
  // rate msg/s/workers
  val rate = 100
  val workers = Runtime.getRuntime().availableProcessors()
  val maxMsg = rate * workers
  System.err.println("Using " + workers + " workers.")
  val timer = 1

  def main(args: Array[String]): Unit = {

    // actors
    val system = ActorSystem("EDIParser")
    val parser = system.actorOf(ConcurrentParser.props(args(0)), name = "parser")

    // start
    parser ! "start"
  }
}
