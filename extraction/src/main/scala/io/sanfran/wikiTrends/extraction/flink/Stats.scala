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
import java.util.concurrent.TimeUnit

import io.sanfran.wikiTrends.extraction.WikiUtils

import org.apache.flink.api.scala.ExecutionEnvironment

import org.apache.flink.api.scala._
import org.apache.flink.api.scala.DataSet
import org.apache.flink.core.fs.FileSystem.WriteMode

object Stats extends App {

  override def main(args: Array[String]) {
    super.main(args)
    calcStats(args(0), args(1))
  }
  
  def calcStats(pageFile : String, outputPath: String) = {

    implicit val env = ExecutionEnvironment.getExecutionEnvironment

    val data = WikiUtils.readWikiTrafficTuple(pageFile, env)
    
    
    //projectName: 0, pageTitle: 1, requestNumber: 2, contentSize: 3, year: 4, month: 5, day: 6, hour: 7
    
    val input = data.filter { t => t._1.equals("en") || t._1.equals("de") }
        
    val sumPerDay = input.groupBy(0,1,4,5,6)
         .reduce((a,b) => (a._1, a._2, a._3 + b._3, a._4 + b._4, a._5, a._6, a._7, a._8))

    sumPerDay.writeAsCsv( outputPath + "day_sums", writeMode = WriteMode.OVERWRITE, fieldDelimiter = " ")
    
    //                                            avg   avg   max   max            min date                          max date
    val stats = sumPerDay.map { a => (a._1, a._2, a._3, a._4, a._3, a._4,          new Date(a._5, (a._6 - 1), a._7), new Date(a._5, (a._6 - 1), a._7))}
                       .groupBy(0,1)
                       .reduce {(a,b) =>
                                  val minDate = if (a._7.before(b._7)) {
                                    a._7
                                  } else  {
                                    b._7
                                  }
                                  val maxDate = if (a._8.after(b._8)) {
                                    a._8
                                  } else  {
                                    b._8
                                  }
                        
                        (a._1, a._2, a._3 + b._3, a._4 + b._4, Math.max(a._3, b._3), Math.max(a._4, b._4), minDate, maxDate) 
                      }.map { a => 
                        val daysBetweenMinMax = TimeUnit.DAYS.convert(Math.abs(a._8.getTime - a._7.getTime), TimeUnit.MILLISECONDS).toFloat + 1.0
                        (a._1, a._2, a._3 / daysBetweenMinMax, a._4 / daysBetweenMinMax, a._5, a._6) 
                      }
           
    stats.writeAsCsv(outputPath + "stats", writeMode = WriteMode.OVERWRITE, fieldDelimiter = " ")
    
    env.execute()
    
  }

}
