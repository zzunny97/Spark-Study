package com.example.chapter7

import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.storage.StorageLevel

object StreamingSecond {
  def main(args: Array[String]) {
    Logger.getRootLogger.setLevel(Level.WARN)
    val sparkConf = new SparkConf().setAppName("StreamingFirst")
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(10))
    val lines = ssc.textFileStream(args(0))
    val words = lines.flatMap(_.split(" ")).filter(_.nonEmpty)
    val wordCounts = words.map((_, 1)).reduceByKey(_ + _)
    wordCounts.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
