/**
 * Track the trackers
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

    val dataHour = data.map(new RichMapFunction[WikiTrafficID, DataHourId]() {
      var beginDate: DateTime = null

      override def open(config: Configuration): Unit = {
        val startDate = getRuntimeContext().getBroadcastVariable[WikiTrafficID]("startDate").iterator().next()

        beginDate = new DateTime(startDate.year, startDate.month, startDate.day, startDate.hour, 0, 0, timezone)        
      }

      def map(t: WikiTrafficID):  DataHourId = {
        
        val currentDate = new DateTime(t.year, t.month, t.day, t.hour, 0, 0, timezone)

        val differenceHours = Hours.hoursBetween(beginDate, currentDate).getHours.toLong

        //new DataHourId(Math.log(t.requestNumber), differenceHours)
        new DataHourId(t.requestNumber, differenceHours)
        
      }
    }).withBroadcastSet(startDate, "startDate")
    
    val dataM1 = dataHour.map { t => new DataHourId(t.logVisits, t.hourId + 1)}
    val dataM2 = dataHour.map { t => new DataHourId(t.logVisits, t.hourId + 2)}
    val dataM3 = dataHour.map { t => new DataHourId(t.logVisits, t.hourId + 3)}
    val dataM24 = dataHour.map { t => new DataHourId(t.logVisits, t.hourId + 24)}
    val dataM48 = dataHour.map { t => new DataHourId(t.logVisits, t.hourId + 48)}

    val regressionData = 
      dataHour.join(dataM1).where("hourId").equalTo("hourId") { (d,d1) => new DataHourIdM1(d.hourId, d.logVisits, d1.logVisits)}
              .join(dataM2).where("hourId").equalTo("hourId") { (d,d2) => new DataHourIdM2(d.hourId, d.logVisits, d.logVisits1, d2.logVisits)}
              .join(dataM3).where("hourId").equalTo("hourId") { (d,d3) => new DataHourIdM3(d.hourId, d.logVisits, d.logVisits1, d.logVisits2, d3.logVisits)}
              .join(dataM24).where("hourId").equalTo("hourId") { (d,d24) => new DataHourIdM24(d.hourId, d.logVisits, d.logVisits1, d.logVisits2, d.logVisits3, d24.logVisits)}
              .join(dataM48).where("hourId").equalTo("hourId") { (d,d48) => new RegressionData(d.logVisits, d.logVisits1, d.logVisits2, d.logVisits3, d.logVisits24, d48.logVisits)}
  
    //regressionData.print()


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

    val mlr = MultipleLinearRegression()
                   .setIterations(10)
                   .setStepsize(0.5)
                   .setConvergenceThreshold(0.01)
                   
    
    mlr.fit(trainingDS)

    val weightList = mlr.weightsOption.get.collect()

    val a = weightList(0)
    
    val srs = mlr.squaredResidualSum(trainingDS).collect().apply(0)

    println("Squared error: " + srs)
    
    val predictions = mlr.predict(testDS)
    
    predictions.print //strange results !!!
    
  }

}
