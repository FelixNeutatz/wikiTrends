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
import io.sanfran.wikiTrends.Config
import io.sanfran.wikiTrends.extraction.WikiUtils

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.configuration.Configuration
import org.apache.flink.ml.common.LabeledVector
import org.apache.flink.ml.math.DenseVector
import org.apache.flink.ml.regression.MultipleLinearRegression

import org.joda.time._

import org.apache.flink.api.scala._
import org.apache.flink.api.scala.DataSet

case class DataHourId(logVisits: Double, hourId: Long)

case class DataHourIdTime(visits: Double, hourId: Long, year: Short, month: Short, day: Short, hour: Short, orginalVisits: Double)

case class DataHourIdM1(hourId: Long, logVisits: Double, logVisits1: Double)
case class DataHourIdM2(hourId: Long, logVisits: Double, logVisits1: Double, logVisits2: Double)
case class DataHourIdM3(hourId: Long, logVisits: Double, logVisits1: Double, logVisits2: Double, logVisits3: Double)
case class DataHourIdM24(hourId: Long, logVisits: Double, logVisits1: Double, logVisits2: Double, logVisits3: Double, logVisits24: Double)
case class DataHourIdM48(hourId: Long, logVisits: Double, logVisits1: Double, logVisits2: Double, logVisits3: Double, logVisits24: Double, logVisits48: Double)

object Regression extends App {

  model(Config.get("Obama.sample.path"))
  
  def model(pageFile : String) = {

    implicit val env = ExecutionEnvironment.getExecutionEnvironment

    val data = WikiUtils.readWikiTrafficCSV(pageFile)
    
    val startDate = data.reduce { (a,b) => 
      var result: WikiTrafficID = a
      if (a.hour < b.hour) { result = a }
      if (a.hour > b.hour) { result = b }
      if (a.day < b.day) { result = a }
      if (a.day > b.day) { result = b }
      if (a.month < b.month) { result = a }
      if (a.month > b.month) { result = b }
      if (a.year < b.year) { result = a }
      if (a.year > b.year) { result = b }
      result
    }


    //TODO: is this right?
    val timezone = DateTimeZone.forID("America/Los_Angeles")

    val dataHour = data.map(new RichMapFunction[WikiTrafficID, DataHourIdTime]() {
      var beginDate: DateTime = null

      override def open(config: Configuration): Unit = {
        val startDate = getRuntimeContext().getBroadcastVariable[WikiTrafficID]("startDate").iterator().next()

        beginDate = new DateTime(startDate.year, startDate.month, startDate.day, startDate.hour, 0, 0, timezone)        
      }

      def map(t: WikiTrafficID):  DataHourIdTime = {
        
        val currentDate = new DateTime(t.year, t.month, t.day, t.hour, 0, 0, timezone)

        val differenceHours = Hours.hoursBetween(beginDate, currentDate).getHours.toLong

        //new DataHourIdTime(Math.log(t.requestNumber), differenceHours, t.year, t.month, t.day, t.hour)
        new DataHourIdTime(t.requestNumber, differenceHours, t.year, t.month, t.day, t.hour, t.requestNumber)
      }
    }).withBroadcastSet(startDate, "startDate")
    
    val dataM1 = dataHour.map { t => new DataHourId(t.visits, t.hourId + 1)}
    val dataM2 = dataHour.map { t => new DataHourId(t.visits, t.hourId + 2)}
    val dataM3 = dataHour.map { t => new DataHourId(t.visits, t.hourId + 3)}
    val dataM24 = dataHour.map { t => new DataHourId(t.visits, t.hourId + 24)}
    val dataM48 = dataHour.map { t => new DataHourId(t.visits, t.hourId + 48)}

    val regressionData1 = 
      dataHour.join(dataM1).where("hourId").equalTo("hourId") { (d,d1) => new DataHourIdM1(d.hourId, d.visits, d1.logVisits)}
              .join(dataM2).where("hourId").equalTo("hourId") { (d,d2) => new DataHourIdM2(d.hourId, d.logVisits, d.logVisits1, d2.logVisits)}
              .join(dataM3).where("hourId").equalTo("hourId") { (d,d3) => new DataHourIdM3(d.hourId, d.logVisits, d.logVisits1, d.logVisits2, d3.logVisits)}
              .join(dataM24).where("hourId").equalTo("hourId") { (d,d24) => new DataHourIdM24(d.hourId, d.logVisits, d.logVisits1, d.logVisits2, d.logVisits3, d24.logVisits)}
              .join(dataM48).where("hourId").equalTo("hourId") { (d,d48) => new DataHourIdM48(d.hourId, d.logVisits, d.logVisits1, d.logVisits2, d.logVisits3, d.logVisits24, d48.logVisits)}

    val regressionData = regressionData1.map { d => new RegressionData(d.logVisits, d.logVisits1, d.logVisits2, d.logVisits3, d.logVisits24, d.logVisits48)}


    val trainingDS = regressionData.map { t => 
      val x = new Array[Double](5)
      x(0) = t.oneHourAgo
      x(1) = t.twoHoursAgo
      x(2) = t.threeHoursAgo
      x(3) = t.twentyFourHoursAgo
      x(4) = t.fourtyEightHoursAgo
      
      new LabeledVector(t.y, new DenseVector(x))
    }
    
    trainingDS.print
    
    val testDS = regressionData.map { t =>
      val x = new Array[Double](5)
      x(0) = t.oneHourAgo
      x(1) = t.twoHoursAgo
      x(2) = t.threeHoursAgo
      x(3) = t.twentyFourHoursAgo
      x(4) = t.fourtyEightHoursAgo

      new DenseVector(x)
    }

    val map1 = new scala.collection.mutable.HashMap[Double, Double]()

    //find best step size by grid search
    for (stepsize <- List(//0.5, 0.3, 0.1, 
                          //0.05, 0.03, 0.01, 
                          //0.005, 0.003, 0.001, 
                          //0.0005, 0.0003, 0.0001, 
                          //0.00005, 0.00003, 0.00001, 
                          //0.000005, 0.000003, 0.000001, 
                          //0.0000005, 0.0000003, 0.0000001,
                          //0.00000005, 0.00000003, 0.00000001,
                          0.000000005, 0.000000003, 0.000000001
                          //0.0000000005, 0.0000000003, 0.0000000001,
                          //0.00000000005, 0.00000000003, 0.00000000001,
                          //0.000000000005, 0.000000000003, 0.000000000001
    )
    ) {

      val mlr = MultipleLinearRegression()
          .setIterations(10)
          .setStepsize(stepsize)

      mlr.fit(trainingDS)

      val srs = mlr.squaredResidualSum(trainingDS).collect().apply(0)
      map1 += new Tuple2(stepsize, srs)

      val predictions = mlr.predict(testDS)
      
      predictions.count()
    }
    
    val bestStepSize = map1.toSeq.sortBy(_._1).reverse(0)._1

    //final run
    val mlr = MultipleLinearRegression()
        .setIterations(10)
        .setStepsize(bestStepSize)


    mlr.fit(trainingDS)

    val weightList = mlr.weightsOption.get.collect()

    val a = weightList(0)

    val srs = mlr.squaredResidualSum(trainingDS).collect().apply(0)
    
    println("Squared error: " + srs)

    val predictions = mlr.predict(testDS)

    predictions.print

    val hourInformation = regressionData1.map { t => new Tuple2[Tuple5[Double,Double,Double,Double,Double],Long]((t.logVisits1, t.logVisits2, t.logVisits3, t.logVisits24, t.logVisits48),t.hourId)}
    
    val predictionsWithTime = predictions.map { t => new Tuple2[Tuple5[Double,Double,Double,Double,Double],Double]((t.vector.apply(0),t.vector.apply(1),t.vector.apply(2),t.vector.apply(3),t.vector.apply(4)),t.label)}
        .distinct()
        .join(hourInformation)
        .where(0)
        .equalTo(0) { (a,b) => (b._2, a._2)} //hour, prediction
        .join(dataHour)
        .where(0)
        .equalTo("hourId") { (a,b) => new DataHourIdTime(a._2, b.hourId, b.year, b.month, b.day, b.hour, b.visits)}

    PlotIT.plotBoth(predictionsWithTime)
    
    //val diff = predictionsWithTime.map( t => new DataHourIdTime(t.orginalVisits - t.visits, t.hourId,t.year, t.month, t.day, t.hour, t.orginalVisits))
    val diff = predictionsWithTime.map( t => new DataHourIdTime(Math.abs(t.orginalVisits - t.visits), t.hourId,t.year, t.month, t.day, t.hour, t.orginalVisits))

    
    
    PlotIT.plotDataPredictions(diff)
    
    val compression = 100
    val pageSize = 100

    val quantile = diff.map { t => 
      //val dist = TDigest.createDigest(compression)
      val dist = new ArrayDigest(pageSize,compression)
      dist.add(t.visits)
      dist
    }.reduce { (a, b) =>
      a.add(b)
      a.compress()
      a
    }.map { t => t.quantile(0.999) }.collect().apply(0)

    PlotIT.plotDiffWithThreshold(diff, quantile)    
  }

}
