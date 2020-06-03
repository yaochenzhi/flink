package com.yaochenzhi.flink

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._

object WordCountStream {
  def main(args: Array[String]): Unit ={

    // create env
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // read from socket created via nc -lk localhost 9000
    val dataStream = env.socketTextStream("localhost", 9000)

    val wordCountDataStream = dataStream.flatMap(_.split(" "))
      .filter(_.nonEmpty)
      .map( (_, 1) )
      .keyBy(0)
      .sum(1)

    wordCountDataStream.print()

    env.execute("Start word count stream job")
  }
}
