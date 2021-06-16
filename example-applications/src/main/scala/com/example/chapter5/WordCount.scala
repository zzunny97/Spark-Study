package com.example.chapter5

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object WordCount {

  def main(args: Array[String]) {

    require(args.length >= 1, "드라이버 프로그램의 인수에 단어를 세고자 하는 파일의 경로를 지정해주세요.")

    val conf = new SparkConf
    val sc = new SparkContext(conf)

    try {
      val filePath = args(0)
      val wordAndCountRDD = sc
        .textFile(filePath)
        .flatMap(_.split("[ ,.]"))
        .filter(_.matches("""\p{Alnum}+"""))
        .map((_, 1))
        .reduceByKey(_ + _)

      wordAndCountRDD.collect().foreach(println)
    } finally {
      sc.stop()
    }
  }
}
