package com.example.chapter4

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.joda.time.format.DateTimeFormat
import org.joda.time.DateTime
import org.joda.time.DateTimeConstants

object SundayCount {

  def main(args: Array[String]) {

    if (args.length < 1) {
      throw new IllegalArgumentException("Usage: spark-submit <filename>")
    }

    val filePath = args(0)
    val conf = new SparkConf
    val sc = new SparkContext(conf)

    try {
      val textRDD = sc.textFile(filePath)
      val dateTimeRDD = textRDD.map { dateStr =>
        val pattern = DateTimeFormat.forPattern("yyyyMMdd")
        DateTime.parse(dateStr, pattern)
      }

      val sundayRDD = dateTimeRDD.filter(dateTime =>
        dateTime.getDayOfWeek() == DateTimeConstants.SUNDAY
      )

      val numOfSunday = sundayRDD.count()
      println(s"주어진 데이터에는 일요일이 ${numOfSunday}개 들어있습니다.")

    } finally {
      sc.stop()
    }

  }

}
