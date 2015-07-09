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
    /*
    if (args.length < 1) {
      println("Please add the path of files as argument")
      return
    }
    val path = args(0)
    */
    val path = "extraction/src/test/resources/"

    var windowSize : Integer = null
    var sliceSize : Integer = null
    if (args.length > 2) {
      windowSize = Integer.parseInt(args(1))
      sliceSize = Integer.parseInt(args(2))
    } else {
      windowSize = 1 //in hours
      sliceSize = 1 //in hours
    }

    var date = new Date(2015, 0, 1)

    val env = ExecutionEnvironment.getExecutionEnvironment

    val data = readFiles(date, windowSize, path, env)
    date = DateUtils.addHours(date, windowSize)
    movingAverage(data, windowSize, sliceSize, date, 1, path, env)

    env.execute()

  }

  def readFiles(date : Date, size : Int, path : String, env : ExecutionEnvironment) = {
    var currDate = date
    var data : DataSet[(String, String, Long, Long, Short, Byte, Byte, Byte)] = null
    for (i <- 0 until size) {
      val fileString = "pagecounts-" + currDate.getYear + String.format("%02d", currDate.getMonth+1 : Integer) + String.format("%02d", currDate.getDate : Integer) + "-" + String.format("%02d", currDate.getHours : Integer) + "*"
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

  def movingAverage(data : DataSet[(String, String, Long, Long, Short, Byte, Byte, Byte)], windowSize: Integer, sliceSize : Integer, date : Date, iterations : Integer, path : String, env : ExecutionEnvironment) = {
    var currDate = date
    var currEndDate = DateUtils.addHours(date, windowSize)
    //val iteration = data.iterate(iterations) {
    for (i <- 0 until iterations) {
      //batch => {
        var currData = data
        val midDate = DateUtils.addHours(currDate, windowSize / 2)
        val average_variance = currData
          .map { a => (a._1, a._2, a._3, a._4, a._3 * a._3, a._4 * a._4) }
          .groupBy(0, 1)
          .reduce { (a, b) => (a._1, a._2, a._3 + b._3, a._4 + b._4, a._5 + a._5, a._6 + a._6) }
          .map { a => (a._1, a._2, a._3.toDouble / windowSize, a._4.toDouble / windowSize, a._5.toDouble / windowSize - (a._3.toDouble / windowSize) * (a._3.toDouble / windowSize), a._6.toDouble / windowSize - (a._4.toDouble / windowSize) * (a._4.toDouble / windowSize), midDate.getYear, midDate.getMonth + 1, midDate.getDate) }

        average_variance.writeAsCsv(path + "averages_variances_" + midDate.getYear + String.format("%02d", midDate.getMonth + 1: Integer) + String.format("%02d", midDate.getDate: Integer) + "-" + String.format("%02d", midDate.getHours: Integer), writeMode = WriteMode.OVERWRITE, fieldDelimiter = " ")

        //read in and union new batch slice (days)
        currDate = DateUtils.addHours(currDate, sliceSize)
        //filter out old batch slice (days)
        var newBatch = currData.filter { a => a._5 > currDate.getYear || (a._5 == currDate.getYear && a._6 > currDate.getMonth + 1) || (a._5 == currDate.getYear && a._6 == currDate.getMonth + 1 && a._7 > currDate.getDate) || (a._5 == currDate.getYear && a._6 == currDate.getMonth + 1 && a._7 == currDate.getDate && a._8 >= currDate.getHours) }
        newBatch = newBatch.union(readFiles(currEndDate, sliceSize, path, env))
        currEndDate = DateUtils.addHours(currEndDate, sliceSize)
        currData = newBatch
      }
    //}
    //}
  }

}