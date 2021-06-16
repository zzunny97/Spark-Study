package com.example.chapter5

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.util.LongAccumulator

object QuestionnaireSummarization {

  /** 모든 앙케이트의 평가 평균을 계산하는 메서드
    */
  private def computeAllAvg(rdd: RDD[(Int, String, Int)]) = {
    val (totalPoint, count) =
      rdd.map(record => (record._3, 1)).reduce {
        case ((intermedPoint, intermedCount), (point, one)) =>
          (intermedPoint + point, intermedCount + one)
      }
    totalPoint / count.toDouble
  }

  /** 연령대별 평가의 평균을 계산하는 메서드
    */
  private def computeAgeRangeAvg(rdd: RDD[(Int, String, Int)]) = {
    rdd
      .map(record => (record._1, (record._3, 1)))
      .reduceByKey { case ((intermedPoint, intermedCount), (point, one)) =>
        (intermedPoint + point, intermedCount + one)
      }
      .map { case (ageRange, (totalPoint, count)) =>
        (ageRange, totalPoint / count.toDouble)
      }
      .collect()
  }

  /** 남녀별 평가의 평균을 구하는 메서드
    */
  private def computeMorFAvg(
      rdd: RDD[(Int, String, Int)],
      numMAcc: LongAccumulator,
      totalPointMAcc: LongAccumulator,
      numFAcc: LongAccumulator,
      totalPointFAcc: LongAccumulator
  ) = {
    rdd.foreach { case (_, maleOrFemale, point) =>
      maleOrFemale match {
        case "M" =>
          numMAcc.add(1)
          totalPointMAcc.add(point)
        case "F" =>
          numFAcc.add(1)
          totalPointFAcc.add(point)
      }

    }
    Seq(
      ("Male", totalPointMAcc.value / numMAcc.value.toDouble),
      ("Female", totalPointFAcc.value / numFAcc.value.toDouble)
    )
  }

  def main(args: Array[String]) = {
    require(
      args.length >= 1,
      """
        |애플리케이션의 인수에
        |<앙케이트 CSV 파일의 경로>
        |<출력하는 결과파일의 경로>를 지정해주세요.""".stripMargin
    )

    val conf = new SparkConf
    val sc = new SparkContext(conf)

    try {
      val filePath = args(0)

      // 앙케이트를 로드해 (연령대, 성별, 평가) 형의
      // 튜플을 요소로 하는 RDD를 생성
      // ageRange: 연령대를 구하기 위해 10으로 나누었다가 다시 곱해줌. e.g. 24 => 20
      val questionnaireRDD = sc.textFile(filePath).map { record =>
        val splitRecord = record.split(",")
        val ageRange = splitRecord(0).toInt / 10 * 10
        val maleOrFemale = splitRecord(1)
        val point = splitRecord(2).toInt

        (ageRange, maleOrFemale, point)
      }

      // questionnaireRDD는 각 집계처리에 이용되므로 캐시에 보존.
      questionnaireRDD.cache()

      // 모든 평가의 평균치를 계산
      val avgAll = computeAllAvg(questionnaireRDD)

      // 연령대별 평균치를 계산
      val avgAgeRange = computeAgeRangeAvg(questionnaireRDD)

      // 성별이 M인 앙케이트의 건수를 세는 어큐뮬레이터
      val numMAcc = sc.longAccumulator("Number of M")
      // 성별이 M인 앙케이트의 평가를 합계하는 어큐뮬레이터
      val totalPointMAcc = sc.longAccumulator("Total Point of M")
      // 성별이 F인 앙케이트의 건수를 세는 어큐뮬레이터
      val numFAcc = sc.longAccumulator("Number of F")
      // 성별이 F인 앙케이트의 평가를 합계하는 어큐뮬레이터
      val totalPointFAcc = sc.longAccumulator("Total Point of F")

      // 남여별 평균치를 계산한다.
      val avgMorF = computeMorFAvg(
        questionnaireRDD,
        numMAcc,
        totalPointMAcc,
        numFAcc,
        totalPointFAcc
      )

      println(s"AVG ALL: $avgAll")
      avgAgeRange.foreach { case (ageRange, avg) =>
        println(s"AVG Age Range($ageRange): $avg")
      }

      avgMorF.foreach { case (mOrF, avg) =>
        println(s"AVG $mOrF: $avg")

      }
    } finally {
      sc.stop()
    }
  }

}
