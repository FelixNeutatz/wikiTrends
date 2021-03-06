/**
 * wikiTrends
 * Copyright (C) 2015  Felix Neutatz, Stephan Alaniz Kupsch 
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package io.sanfran.wikiTrends.extraction.flink

import com.tdunning.math.stats.ArrayDigest
import io.sanfran.wikiTrends.extraction.WikiUtils

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.configuration.Configuration
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.ml.common.LabeledVector
import org.apache.flink.ml.math.DenseVector
import org.apache.flink.ml.regression.MultipleLinearRegression
import org.apache.flink.ml._

import java.util.Date

import org.apache.flink.api.scala._
import org.apache.flink.api.scala.DataSet

case class DataHourId(logVisits: Double, hourId: Long)

case class DataHourIdTime(visits: Double, hourId: Long, year: Short, month: Short, day: Short, hour: Short, orginalVisits: Double)

case class TwoSeriesPlot(series1: Double, series2: Double, year: Short, month: Short, day: Short, hour: Short)
case class ThreeSeriesPlot(series1: Double, series2: Double, series3: Double, year: Short, month: Short, day: Short, hour: Short)
case class FourSeriesPlot(series1: Double, series2: Double, series3: Double, series4: Double, year: Short, month: Short, day: Short, hour: Short)


case class DataHourIdM1(hourId: Long, logVisits: Double, logVisits1: Double)
case class DataHourIdM2(hourId: Long, logVisits: Double, logVisits1: Double, logVisits2: Double)
case class DataHourIdM3(hourId: Long, logVisits: Double, logVisits1: Double, logVisits2: Double, logVisits3: Double)
case class DataHourIdM24(hourId: Long, logVisits: Double, logVisits1: Double, logVisits2: Double, logVisits3: Double, logVisits24: Double)
case class DataHourIdM48(hourId: Long, logVisits: Double, logVisits1: Double, logVisits2: Double, logVisits3: Double, logVisits24: Double, logVisits48: Double)

object Regression extends App {

  override def main(args: Array[String]) {
    super.main(args)
    model(args(0), args(1), args(2))
  }

  def applyRegression(data: DataSet[WikiTrafficID], gridSearch: Boolean = false) = {
    val startDate = data.reduce { (a, b) =>
      var result: WikiTrafficID = a
      if (a.hour < b.hour) {
        result = a
      }
      if (a.hour > b.hour) {
        result = b
      }
      if (a.day < b.day) {
        result = a
      }
      if (a.day > b.day) {
        result = b
      }
      if (a.month < b.month) {
        result = a
      }
      if (a.month > b.month) {
        result = b
      }
      if (a.year < b.year) {
        result = a
      }
      if (a.year > b.year) {
        result = b
      }
      result
    }

    //println("startdate: " + startDate.collect())

    val dataHour = data.map(new RichMapFunction[WikiTrafficID, DataHourIdTime]() {
      var beginDate: Date = null

      override def open(config: Configuration): Unit = {

        val startDate = getRuntimeContext().getBroadcastVariable[WikiTrafficID]("startDate").iterator().next()

        beginDate = new Date(startDate.year, startDate.month -1 , startDate.day, startDate.hour, 0)
      }

      def map(t: WikiTrafficID): DataHourIdTime = {

        val currentDate = new Date(t.year, t.month - 1, t.day, t.hour, 0)

        val differenceHours = DateUtils.diffHours(beginDate, currentDate)

        //new DataHourIdTime(Math.log(t.requestNumber), differenceHours, t.year, t.month, t.day, t.hour)
        new DataHourIdTime(t.requestNumber, differenceHours, t.year, t.month, t.day, t.hour, t.requestNumber)
      }
    }).withBroadcastSet(startDate, "startDate")

    val dataM1 = dataHour.map { t => new DataHourId(t.visits, t.hourId + 1) }
    val dataM2 = dataHour.map { t => new DataHourId(t.visits, t.hourId + 2) }
    val dataM3 = dataHour.map { t => new DataHourId(t.visits, t.hourId + 3) }
    val dataM24 = dataHour.map { t => new DataHourId(t.visits, t.hourId + 24) }
    val dataM48 = dataHour.map { t => new DataHourId(t.visits, t.hourId + 48) }

    val regressionData1 =
      dataHour.join(dataM1).where("hourId").equalTo("hourId") { (d, d1) => new DataHourIdM1(d.hourId, d.visits, d1.logVisits) }

          .join(dataM2).where("hourId").equalTo("hourId") { (d, d2) => new DataHourIdM2(d.hourId, d.logVisits, d.logVisits1, d2.logVisits) }
          .join(dataM3).where("hourId").equalTo("hourId") { (d, d3) => new DataHourIdM3(d.hourId, d.logVisits, d.logVisits1, d.logVisits2, d3.logVisits) }
          .join(dataM24).where("hourId").equalTo("hourId") { (d, d24) => new DataHourIdM24(d.hourId, d.logVisits, d.logVisits1, d.logVisits2, d.logVisits3, d24.logVisits) }
          .join(dataM48).where("hourId").equalTo("hourId") { (d, d48) => new DataHourIdM48(d.hourId, d.logVisits, d.logVisits1, d.logVisits2, d.logVisits3, d.logVisits24, d48.logVisits) }

    val regressionData = regressionData1.map { d => new RegressionData(d.logVisits, d.logVisits1, d.logVisits2, d.logVisits3, d.logVisits24, d.logVisits48) }


    val statistics = dataHour.map {x => (x.visits, x.visits*x.visits, 1L)}
            .reduce((a,b) => (a._1 + b._1, a._2 + b._2, a._3 + b._3))
            .map{ t => (t._1 / t._3.toDouble, Math.sqrt((t._2 / t._3) - ((t._1 / t._3.toDouble)*(t._1 / t._3.toDouble))))}
            .collect().head
    
    val mean = -1.0 * statistics._1
    val stddev = statistics._2
    
    val trainingDS = regressionData.map { t =>
      val x = new Array[Double](6)
      x(0) = (t.oneHourAgo + mean) / stddev
      x(1) = (t.twoHoursAgo + mean) / stddev
      x(2) = (t.threeHoursAgo + mean) / stddev
      x(3) = (t.twentyFourHoursAgo + mean) / stddev
      x(4) = (t.fourtyEightHoursAgo + mean) / stddev
      x(5) = 1

      new LabeledVector((t.y+ mean) / stddev, new DenseVector(x))
    }

    //trainingDS.print

    val testDS = trainingDS.map { t => t.vector}

    var bestStepSize = 1.0

    if (gridSearch) {

      val map1 = new scala.collection.mutable.HashMap[Double, Double]()

      //find best step size by grid search
      for (stepsize <- List(
      500.0, 300.0, 100.0,
      50.0, 30.0, 10.0,
      5.0,3.0,1.0,
      0.5, 0.3, 0.1, 
      0.05, 0.03, 0.01,
      0.005, 0.003, 0.001
      )
      ) {

        val mlr = MultipleLinearRegression()
            .setIterations(20)
            .setStepsize(stepsize)

        mlr.fit(trainingDS)

        val srs = mlr.squaredResidualSum(trainingDS).collect().apply(0)
        map1 += new Tuple2(stepsize, srs)
        
        println("stepsize: " + stepsize + " -> " + srs)
      }
      
      bestStepSize = map1.toSeq.sortBy(_._2).head._1

      println("stepsize: " + bestStepSize + " here ho ")
    }
    
    //final run
    val mlr = MultipleLinearRegression()
        .setIterations(20)
        .setStepsize(bestStepSize)


    mlr.fit(trainingDS)

    val predictions = mlr.predict(testDS)

    //predictions.print

    val hourInformation = regressionData1.map { t => new Tuple2[Tuple5[Double, Double, Double, Double, Double], Long]((t.logVisits1, t.logVisits2, t.logVisits3, t.logVisits24, t.logVisits48), t.hourId) }

    val predictionsWithTime = predictions.map { t => new Tuple2[Tuple5[Double, Double, Double, Double, Double], Double](((t.vector.apply(0) * stddev) - mean, (t.vector.apply(1) * stddev) - mean, (t.vector.apply(2) * stddev) - mean, (t.vector.apply(3) * stddev) - mean, (t.vector.apply(4) * stddev) - mean), ((t.label * stddev) - mean)) }
        .distinct()
        .join(hourInformation)
        .where(0)
        .equalTo(0) { (a, b) => (b._2, a._2) } //hour, prediction
        .join(dataHour)
        .where(0)
        .equalTo("hourId") { (a, b) => new DataHourIdTime(a._2, b.hourId, b.year, b.month, b.day, b.hour, b.visits) }

    //val diff = predictionsWithTime.map( t => new DataHourIdTime(t.orginalVisits - t.visits, t.hourId,t.year, t.month, t.day, t.hour, t.orginalVisits))
    val diff = predictionsWithTime.map(t => (t.orginalVisits, t.visits, Math.abs(t.orginalVisits - t.visits), t.year, t.month, t.day, t.hour))

    /*
    val compression = 100
    val pageSize = 100

    
    val quantile = diff.map { t =>
      //val dist = TDigest.createDigest(compression)
      val dist = new ArrayDigest(pageSize, compression)
      dist.add(t._3)
      dist
    }.reduce { (a, b) =>
      a.add(b)
      a.compress()
      a
    }.map { t => t.quantile(0.999) }.collect().head
    */

    val nStandarddev = 3
    val threshold = diff.map { t => (t._3, t._3 * t._3, 1L) }
        .reduce { (a,b) => (a._1 + b._1, a._2 + b._2, a._3 + b._3) }
        .map { t => nStandarddev * Math.sqrt( (t._2 / t._3) - ((t._1 / t._3) * (t._1 / t._3)) ) + (t._1 / t._3) }
        .collect().head

    (diff, threshold)
  }

  def model(pageFile : String, projectName: String, outputPath: String) = {

    implicit val env = ExecutionEnvironment.getExecutionEnvironment


    val indata = WikiUtils.readWikiTrafficCSV(pageFile, " ").filter( t => t.projectName.equals(projectName))

    val titles = indata.map(t => Tuple1(t.pageTitle))
        .distinct(0)
        .collect()

    var allAnomaliesPerDay : DataSet[AnomaliesPerDay] = null

    var iteration = 0
    for (page <- titles) {

      val data = indata.filter(t => t.pageTitle.equals(page._1))

      val result = applyRegression(data)

      val diff = result._1
      val threshold = result._2

      val anomaliesPerDay = diff.filter ( t => t._3 > threshold)
          .groupBy(3,4,5)
          .reduce{ (a,b) =>
        (Math.max(a._1,b._1), Math.max(a._2,b._2), Math.max(a._3,b._3), a._4, a._5, a._6, a._7)
      }
          .map {t => new AnomaliesPerDay(projectName, page._1, t._4, t._5.toByte, t._6.toByte, t._1.toLong, t._3 / threshold, t._1 / t._2)}
          .filter { a => a.relativeToQuantile > 1 }

      anomaliesPerDay.writeAsCsv(outputPath + "anomalies_for_" + iteration, writeMode = WriteMode.OVERWRITE, fieldDelimiter = " ")
      iteration = iteration + 1

    }

    env.execute()
  }

}
