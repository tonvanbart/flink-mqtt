package io.github.tonvanbart

import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility
import com.fasterxml.jackson.annotation.PropertyAccessor
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.configuration.Configuration
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.wikiedits.{WikipediaEditEvent, WikipediaEditsSource}
import org.eclipse.paho.client.mqttv3.MqttClient
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence
import org.slf4j.{Logger, LoggerFactory}

/**
 * Based on Flink Wikipedia edits example, converted to Scala.
 */
object StreamingJob {

  def main(args: Array[String]) {
    // set up the streaming execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val result = env
        .addSource(new WikipediaEditsSource())
        .keyBy(_.getUser)
        .timeWindow(Time.seconds(5))
        .aggregate(new EditAggregator)

// uncomment following to see results in pipeline
//    result.print()

    result.addSink(new MqttSink("github.tonvanbart/wikiedits", "tcp://broker.hivemq.com:1883"))

    // execute program
    env.execute("Flink Streaming Scala API Skeleton")
  }
}

class EditAggregator extends AggregateFunction[WikipediaEditEvent, (String, Long), (String, Long)] {
  override def createAccumulator(): (String, Long) = Tuple2("", 0L)

  override def add(in: WikipediaEditEvent, acc: (String, Long)): (String, Long) = Tuple2(in.getUser, acc._2 + in.getByteDiff)

  override def getResult(acc: (String, Long)): (String, Long) = acc

  override def merge(acc: (String, Long), acc1: (String, Long)): (String, Long) = Tuple2(acc._1, acc._2 + acc1._2)
}

/**
 * Very basic first approach
 */
class MqttSink(val topic: String, val url: String) extends RichSinkFunction[(String, Long)] {

  private val log: Logger = LoggerFactory.getLogger(classOf[MqttSink])

  println(s"MqttSink: Initialize($topic,$url)")
  log.info("Initialize({},{})", topic:Any, url:Any)  // nasty - https://github.com/typesafehub/scalalogging/issues/16
  val mapper = new ObjectMapper()
  mapper.setVisibility(PropertyAccessor.FIELD, Visibility.ANY)
  var client: MqttClient = _

  override def open(parameters: Configuration): Unit = {
    println("MqttSink: open({})")
    super.open(parameters)
    client = new MqttClient(url, MqttClient.generateClientId(), new MemoryPersistence)
    client.connect()
  }

  override def invoke(value: (String, Long), context: SinkFunction.Context[_]): Unit = {
    println(s"MqttSink: invoke($value)")
    def payload = mapper.writeValueAsBytes(new WikiEdit(value._1, value._2))
    client.publish(topic, payload, 2, false)
  }
}

class WikiEdit(var author: String, var delta: Long)
