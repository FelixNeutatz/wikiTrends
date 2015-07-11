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

import java.util.Date

import io.sanfran.wikiTrends.extraction.WikiUtils
import org.apache.flink.api.common.functions.{RichFlatMapFunction, RichMapFunction}
import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.util.Collector


object MovingAverageFlinkIteration extends App {

  override def main(args: Array[String]) {
    super.main(args)

    if (args.length < 2) {
      println("Please add the path of files as argument")
      return
    }
    val input_path = args(0)
    val output_path = args(1)

    var windowSize: Int = 0
    var sliceSize: Int = 0
    //var kernel: String = ""
    if (args.length > 2) {
      windowSize = Integer.parseInt(args(2))
      sliceSize = Integer.parseInt(args(3))
      //kernel = args(4)
    } else {
      windowSize = 7 * 24 //in hours
      sliceSize = 24 //in hours
      //kernel = "identity"
    }

    implicit val env = ExecutionEnvironment.getExecutionEnvironment

    // 1        2     3       4       5     6     7   8
    // project  name  counts  traffic year  month day hour
    val data = WikiUtils.readWikiTrafficCSVTuple(input_path, " ")

    // 1        2     3      4       5               6               7               8                9           10            11               12                13    14    15  16
    // project  name  counts traffic average_counts  average_traffic variance_counts variance_traffic diff_counts diff_traffic  times_std_counts times_std_traffic year  month day hour
    val anomalyData = applyMovingAverage(data, windowSize, sliceSize, env)

    val anomaliesPerDay = anomalyData
      .map( a=> (a._1, a._2, a._3, a._11, a._3 / a._5, a._13, a._14, a._15))
      .groupBy(0,1,4,5,6)
      .reduce{
        (a,b) => (a._1, a._2, Math.max(a._3, b._3), Math.max(a._4, b._4), Math.max(a._5, b._5), a._6, a._7, a._8)
      }.map(a => new AnomaliesPerDay(a._1, a._2, a._6, a._7, a._8, a._3, a._4, a._5))
      .filter(t => t.relativeToQuantile > 3)

    anomaliesPerDay.writeAsCsv(output_path + "allAnomalies", writeMode = WriteMode.OVERWRITE, fieldDelimiter = " ")

    //print anomalies
    /*
    val anomalies2std = anomalyData.filter(t => t._11 > 2)

    anomalies2std.writeAsCsv(output_path + "anomalies2std", writeMode = WriteMode.OVERWRITE, fieldDelimiter = " ")

    val anomalies3std = anomalyData.filter(t => t._11 > 3)

    anomalies3std.writeAsCsv(output_path + "anomalies3std", writeMode = WriteMode.OVERWRITE, fieldDelimiter = " ")

    val anomalies4std = anomalyData.filter(t => t._11 > 4)

    anomalies4std.writeAsCsv(output_path + "anomalies4std", writeMode = WriteMode.OVERWRITE, fieldDelimiter = " ")

    val anomalies5std = anomalyData.filter(t => t._11 > 5)

    anomalies5std.writeAsCsv(output_path + "anomalies5std", writeMode = WriteMode.OVERWRITE, fieldDelimiter = " ")

    val anomalies10std = anomalyData.filter(t => t._11 > 6)

    anomalies10std.writeAsCsv(output_path + "anomalies10std", writeMode = WriteMode.OVERWRITE, fieldDelimiter = " ")
    */

    env.execute()

  }

  def applyMovingAverage(data: DataSet[(String, String, Long, Long, Short, Byte, Byte, Byte)], windowSize: Integer, sliceSize: Integer, env: ExecutionEnvironment): DataSet[(String, String, Long, Long, Double, Double, Double, Double, Double, Double, Double, Double, Short, Byte, Byte, Byte)] = {
    val startDateSet = data.reduce { (a, b) =>
      var result: (String, String, Long, Long, Short, Byte, Byte, Byte) = a
      if (a._8 < b._8) {
        // hour
        result = a
      }
      if (a._8 > b._8) {
        result = b
      }
      if (a._7 < b._7) {
        //day
        result = a
      }
      if (a._7 > b._7) {
        result = b
      }
      if (a._6 < b._6) {
        //month
        result = a
      }
      if (a._6 > b._6) {
        result = b
      }
      if (a._5 < b._5) {
        //year
        result = a
      }
      if (a._5 > b._5) {
        result = b
      }
      result
    }

    val endDateSet = data.reduce { (a, b) =>
      var result: (String, String, Long, Long, Short, Byte, Byte, Byte) = a
      if (a._8 > b._8) {
        // hour
        result = a
      }
      if (a._8 < b._8) {
        result = b
      }
      if (a._7 > b._7) {
        //day
        result = a
      }
      if (a._7 < b._7) {
        result = b
      }
      if (a._6 > b._6) {
        //month
        result = a
      }
      if (a._6 < b._6) {
        result = b
      }
      if (a._5 > b._5) {
        //year
        result = a
      }
      if (a._5 < b._5) {
        result = b
      }
      result
    }


    val startDate = startDateSet.collect().head
    val endDate = endDateSet.collect().head
    val finalEndDate = new Date(endDate._5, endDate._6 - 1, endDate._7, endDate._8, 0)
    val initalStartDate = new Date(startDate._5, startDate._6 - 1, startDate._7, startDate._8, 0)

    val averages_variances: DataSet[(String, String, Double, Double, Double, Double, Short, Byte, Byte)] = env.fromCollection(List())

    val iterations = (DateUtils.diffDays(initalStartDate, finalEndDate) - windowSize / 24) + 1

    //println("Iterations: " + iterations)
    //println("DiffDays: " + DateUtils.diffDays(initalStartDate, finalEndDate))

    val result = averages_variances.iterateDelta(data, iterations.toInt, Array(0, 1, 6, 7, 8)) {

      (averages_variances, dataIter) =>
        val filteredData = dataIter.flatMap(new WindowFilter(windowSize, sliceSize)).withBroadcastSet(startDateSet, "startdate")

        // 1        2     3               4               5               6                 7     8     9
        // project  name  average_counts  average_traffic variance_counts variance_traffic  year  month day
        val average_variance = filteredData
          .map { a => (a._1, a._2, a._3, a._4, a._3 * a._3, a._4 * a._4, 1L) }
          .groupBy(0, 1)
          .reduce { (a, b) => (a._1, a._2, a._3 + b._3, a._4 + b._4, a._5 + b._5, a._6 + b._6, a._7 + b._7) }
          .filter { t => t._7 > 1 }
          .map(new AverageVarianceCalculator(windowSize, sliceSize)).withBroadcastSet(startDateSet, "startdate")
          .filter(t => t._5 != 0 && t._6 != 0)

        val new_averages_variances = averages_variances.coGroup(average_variance).where(0, 1, 6, 7, 8).equalTo(0, 1, 6, 7, 8) {
          (oldData, newData, out: Collector[(String, String, Double, Double, Double, Double, Short, Byte, Byte)]) =>

            for (oD <- oldData) {
              out.collect(oD)
            }

            for (nD <- newData) {
              out.collect(nD)
            }
        }
        (new_averages_variances, dataIter)
    }

    //println("Result count: " + result.count())
    //println("Data count: " + data.count())

    val anomalyData = result.join(data).where(0, 1, 6, 7, 8).equalTo(0, 1, 4, 5, 6) {
      // 1        2     3      4       5               6               7               8                9           10            11               12                13    14    15  16
      // project  name  counts traffic average_counts  average_traffic variance_counts variance_traffic diff_counts diff_traffic  times_std_counts times_std_traffic year  month day hour
      (a, b) => (a._1, a._2, b._3, b._4, a._3, a._4, a._5, a._6, Math.abs(a._3 - b._3), Math.abs(a._4 - b._4), Math.abs(a._3 - b._3) / Math.sqrt(a._5), Math.abs(a._4 - b._4) / Math.sqrt(a._6), a._7, a._8, a._9, b._8)
    }

    anomalyData
  }

  class AverageVarianceCalculator(windowSize : Int, sliceSize : Int) extends RichMapFunction[(String, String, Long, Long, Long, Long, Long), (String, String, Double, Double, Double, Double, Short, Byte, Byte)] {
    var midDate : Date = null

    override def open(config: Configuration): Unit = {
      val currentIteration = getIterationRuntimeContext().getSuperstepNumber() - 1
      val startDateSet = getRuntimeContext.getBroadcastVariable[(String, String, Long, Long, Short, Byte, Byte, Byte)]("startdate").get(0)
      midDate = new Date(startDateSet._5, startDateSet._6 - 1, startDateSet._7, startDateSet._8, 0)
      midDate = DateUtils.addHours(midDate, currentIteration * sliceSize + windowSize / 2)
      //println("midDate: " + midDate + " Iteration: " + currentIteration)
    }
    // 1        2     3               4               5               6                 7     8     9
    // project  name  average_counts  average_traffic variance_counts variance_traffic  year  month day
    def map(in: (String, String, Long, Long, Long, Long, Long)): (String, String, Double, Double, Double, Double, Short, Byte, Byte) = {
      (in._1, in._2, in._3.toDouble / in._7, in._4.toDouble / in._7, in._5.toDouble / in._7 - (in._3.toDouble / in._7) * (in._3.toDouble / in._7), in._6.toDouble / in._7 - (in._4.toDouble / in._7) * (in._4.toDouble / in._7), midDate.getYear.toShort, (midDate.getMonth + 1).toByte, midDate.getDate.toByte)
    }
  }

  class WindowFilter(windowSize : Int, sliceSize : Int) extends RichFlatMapFunction[(String, String, Long, Long, Short, Byte, Byte, Byte),(String, String, Long, Long, Short, Byte, Byte, Byte)] {
    var startDate : Date = null
    var endDate : Date = null
    var i : Int = 0
    var currentIteration : Int  = 0

    override def open(config: Configuration): Unit = {
      i = 0
      currentIteration = getIterationRuntimeContext().getSuperstepNumber() - 1
      val startDateSet = getRuntimeContext.getBroadcastVariable[(String, String, Long, Long, Short, Byte, Byte, Byte)]("startdate").get(0)
      startDate = new Date(startDateSet._5, startDateSet._6 - 1, startDateSet._7, startDateSet._8, 0)
      startDate = DateUtils.addHours(startDate, currentIteration * sliceSize)
      endDate = DateUtils.addHours(startDate, windowSize)
      //println("startDate: " + startDate + " endDate: " + endDate + " Iteration: " + currentIteration)
    }
    override def flatMap(in: (String, String, Long, Long, Short, Byte, Byte, Byte), collector: Collector[(String, String, Long, Long, Short, Byte, Byte, Byte)]): Unit = {
      val date = new Date(in._5, in._6 - 1, in._7, in._8, 0)
      if(!date.before(startDate) && !date.after(endDate)) {
        collector.collect(in)
        i += 1
      }
    }

    override def close(): Unit = {
      //println("Iteration: " + currentIteration + " DataSize:" + i)
    }
  }

  def epanechnikovKernel(u: Int): Double = {
    if(u < -1 || u > 1) {
      return 0D
    }
    return 3D/4D * (1 - u * u)
  }
}