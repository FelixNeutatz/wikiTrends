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
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem.WriteMode


object MovingAverage extends App {

  override def main(args: Array[String]) {
    super.main(args)

    if (args.length < 2) {
      println("Please add the path of files as argument")
      return
    }
    val input_path = args(0)
    val output_path = args(1)

    var windowSize: Integer = null
    var sliceSize: Integer = null
    if (args.length > 2) {
      windowSize = Integer.parseInt(args(2))
      sliceSize = Integer.parseInt(args(3))
    } else {
      windowSize = 7 * 24 //in hours
      sliceSize = 24 //in hours
    }

    implicit val env = ExecutionEnvironment.getExecutionEnvironment

    // 1        2     3       4       5     6     7   8
    // project  name  counts  traffic year  month day hour
    val data = WikiUtils.readWikiTrafficCSVTuple(input_path, " ")

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
    var currStartDate = new Date(startDate._5, startDate._6 - 1, startDate._7, startDate._8, 0)
    var currEndDate = DateUtils.addHours(currStartDate, windowSize)

    //var full_averages_variances: DataSet[(String, String, Double, Double, Double, Double, Short, Byte, Byte)] = null

    while (currEndDate.before(finalEndDate)) {
      val currMidDate = DateUtils.addHours(currStartDate, windowSize / 2)

      val filteredData = data.filter {
        t =>
          val date = new Date(t._5, t._6 - 1, t._7, t._8, 0)
          !date.before(currStartDate) && !date.after(currEndDate)
      }

      //println("filtered data count: " + filteredData.map( t => (t._1, t._2, 1)).groupBy(0,1).sum(2).collect())

      // 1        2     3               4               5               6                 7     8     9
      // project  name  average_counts  average_traffic variance_counts variance_traffic  year  month day
      val average_variance = filteredData
        .map { a => (a._1, a._2, a._3, a._4, a._3 * a._3, a._4 * a._4, 1) }
        .groupBy(0, 1)
        .reduce { (a, b) => (a._1, a._2, a._3 + b._3, a._4 + b._4, a._5 + b._5, a._6 + b._6, a._7 + b._7) }
        .filter { t => t._7 > 1}
        .map { a => (a._1, a._2, a._3.toDouble / a._7, a._4.toDouble / a._7, a._5.toDouble / a._7 - (a._3.toDouble / a._7) * (a._3.toDouble / a._7), a._6.toDouble / a._7 - (a._4.toDouble / a._7) * (a._4.toDouble / a._7), currMidDate.getYear.toShort, (currMidDate.getMonth + 1).toByte, currMidDate.getDate.toByte) }

      //println(average_variance.collect())

      average_variance.writeAsCsv(output_path + "averages_variances_" + currMidDate.getYear + String.format("%02d", currMidDate.getMonth + 1: Integer) + String.format("%02d", currMidDate.getDate: Integer) + "-" + String.format("%02d", currMidDate.getHours: Integer), writeMode = WriteMode.OVERWRITE, fieldDelimiter = " ")

      /*
      if (full_averages_variances == null) {
        full_averages_variances = average_variance
      } else {
        full_averages_variances = full_averages_variances.union(average_variance)
      }
      */


      currStartDate = DateUtils.addHours(currStartDate, sliceSize)
      currEndDate = DateUtils.addHours(currEndDate, sliceSize)
    }

    /*
    val anomalyData = full_averages_variances.join(data).where(0, 1, 6, 7, 8).equalTo(0, 1, 4, 5, 6) {
      // 1        2     3      4       5           6             7                8                 9     10    11  12
      // project  name  counts traffic diff_counts diff_traffic  times_std_counts times_std_traffic year  month day hour
      (a, b) => (a._1, a._2, b._3, b._4, Math.abs(a._3 - b._3), Math.abs(a._4 - b._4), Math.abs(a._3 - b._3) / Math.sqrt(a._5), Math.abs(a._4 - b._4) / Math.sqrt(a._6), a._7, a._8, a._9, b._8)
    }

    val anomalies2std = anomalyData.filter(t => t._7 > 2 || t._8 > 2)

    anomalies2std.writeAsCsv(output_path + "anomalies2std", writeMode = WriteMode.OVERWRITE, fieldDelimiter = " ")

    val anomalies3std = anomalyData.filter(t => t._7 > 3 || t._8 > 3)

    anomalies3std.writeAsCsv(output_path + "anomalies3std", writeMode = WriteMode.OVERWRITE, fieldDelimiter = " ")

    val anomalies4std = anomalyData.filter(t => t._7 > 4 || t._8 > 4)

    anomalies4std.writeAsCsv(output_path + "anomalies4std", writeMode = WriteMode.OVERWRITE, fieldDelimiter = " ")

    val anomalies5std = anomalyData.filter(t => t._7 > 5 || t._8 > 5)

    anomalies5std.writeAsCsv(output_path + "anomalies5std", writeMode = WriteMode.OVERWRITE, fieldDelimiter = " ")

    val anomalies10std = anomalyData.filter(t => t._7 > 10 || t._8 > 10)

    anomalies10std.writeAsCsv(output_path + "anomalies10std", writeMode = WriteMode.OVERWRITE, fieldDelimiter = " ")

    //movingAverage(data, windowSize, sliceSize, date, 1, path, env)
    */
    env.execute()

  }

  def readFiles(date: Date, size: Int, path: String, env: ExecutionEnvironment) = {
    var currDate = date
    var data: DataSet[(String, String, Long, Long, Short, Byte, Byte, Byte)] = null
    for (i <- 0 until size) {
      val fileString = "pagecounts-" + currDate.getYear + String.format("%02d", currDate.getMonth + 1: Integer) + String.format("%02d", currDate.getDate: Integer) + "-" + String.format("%02d", currDate.getHours: Integer) + "*"
      val newData = WikiUtils.readWikiTrafficTuple(path + fileString, env)
      if (i == 0) {
        data = newData
      } else {
        data = data.union(newData)
      }
      currDate = DateUtils.addHours(currDate, 1)
    }
    data
  }

  def movingAverage(data: DataSet[(String, String, Long, Long, Short, Byte, Byte, Byte)], windowSize: Integer, sliceSize: Integer, date: Date, iterations: Integer, path: String, env: ExecutionEnvironment) = {
    var currDate = date
    var currEndDate = DateUtils.addHours(date, windowSize)
    val iteration = data.iterate(iterations) {
      batch => {
        val midDate = DateUtils.addHours(currDate, windowSize / 2)
        val average_variance = batch
          .map { a => (a._1, a._2, a._3, a._4, a._3 * a._3, a._4 * a._4) }
          .groupBy(0, 1)
          .reduce { (a, b) => (a._1, a._2, a._3 + b._3, a._4 + b._4, a._5 + a._5, a._6 + a._6) }
          .map { a => (a._1, a._2, a._3.toDouble / windowSize, a._4.toDouble / windowSize, a._5.toDouble / windowSize - (a._3.toDouble / windowSize) * (a._3.toDouble / windowSize), a._6.toDouble / windowSize - (a._4.toDouble / windowSize) * (a._4.toDouble / windowSize), midDate.getYear, midDate.getMonth + 1, midDate.getDate) }

        average_variance.writeAsCsv(path + "averages_variances_" + midDate.getYear + String.format("%02d", midDate.getMonth + 1: Integer) + String.format("%02d", midDate.getDate: Integer) + "-" + String.format("%02d", midDate.getHours: Integer), writeMode = WriteMode.OVERWRITE, fieldDelimiter = " ")

        //read in and union new batch slice (days)
        currDate = DateUtils.addHours(currDate, sliceSize)
        //filter out old batch slice (days)
        var newBatch = batch.filter { a => a._5 > currDate.getYear || (a._5 == currDate.getYear && a._6 > currDate.getMonth + 1) || (a._5 == currDate.getYear && a._6 == currDate.getMonth + 1 && a._7 > currDate.getDate) || (a._5 == currDate.getYear && a._6 == currDate.getMonth + 1 && a._7 == currDate.getDate && a._8 >= currDate.getHours) }
        newBatch = newBatch.union(readFiles(currEndDate, sliceSize, path, env))
        currEndDate = DateUtils.addHours(currEndDate, sliceSize)
        newBatch
      }
      //}
      //}
    }

  }
}