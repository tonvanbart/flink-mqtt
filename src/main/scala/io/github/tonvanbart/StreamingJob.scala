/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.github.tonvanbart

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper
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
  var client: MqttClient = _

  override def open(parameters: Configuration): Unit = {
    println("MqttSink: open({})")
    super.open(parameters)
    client = new MqttClient(url, MqttClient.generateClientId(), new MemoryPersistence)
    client.connect()
  }

  override def invoke(value: (String, Long), context: SinkFunction.Context[_]): Unit = {
    println(s"MqttSink: invoke($value)")
    def payload = mapper.writeValueAsBytes(value)
    client.publish(topic, payload, 2, false)
  }
}

class WikiEdit(author: String, delta: Long)
