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
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.wikiedits.{WikipediaEditEvent, WikipediaEditsSource}

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

    result.print()

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
