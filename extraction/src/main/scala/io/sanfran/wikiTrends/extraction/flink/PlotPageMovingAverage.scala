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

import io.sanfran.wikiTrends.extraction.WikiUtils
import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.core.fs.FileSystem.WriteMode

object PlotPageMovingAverage extends App {

  var windowSize: Int = 0
  var sliceSize: Int = 0

  override def main(args: Array[String]) {
    super.main(args)

    if (args.length > 4) {
      windowSize = Integer.parseInt(args(4))
      sliceSize = Integer.parseInt(args(5))
    } else {
      windowSize = 7 * 24 //in hours
      sliceSize = 24 //in hours
    }

    plotPage(args(0), args(1), args(2), args(3), args(6).toBoolean)
  } 

  def plotPage(inputPath : String, outputPath: String, projectName: String, page: String, generatePlots: Boolean) = {

    implicit val env = ExecutionEnvironment.getExecutionEnvironment

    val data = WikiUtils.readWikiTrafficCSVTuple(inputPath, " ").filter( t => t._1.equals(projectName) && t._2.equals(page))

    if (data.count() == 0) {
      throw new Exception("Page not found")
    }
    
    if (!generatePlots) {
      data.writeAsCsv(outputPath + "plot", writeMode = WriteMode.OVERWRITE, fieldDelimiter = " ")
      env.execute()
    }
    else {
      // 1        2     3      4       5               6               7               8                9           10            11               12                13    14    15  16
      
      // project  name  counts traffic average_counts  average_traffic variance_counts variance_traffic diff_counts diff_traffic  times_std_counts times_std_traffic year  month day hour
      val result = MovingAverageFlinkIteration.applyMovingAverage(data, windowSize, sliceSize, env)

      val model = result.map { t => TwoSeriesPlot(t._3, t._5, t._13, t._14, t._15, t._16) }

      PlotIT.plotBoth(model, "original wikitraffic", "moving average model", page, outputPath)

      val diffWithThreshold = result.map { t => TwoSeriesPlot(t._9, 3 * Math.sqrt(t._7), t._13, t._14, t._15, t._16) }

      PlotIT.plotBoth(diffWithThreshold, "Difference: original wikitraffic - regression model", "threshold", page, outputPath)

      val alertFunction = result.map { t => TwoSeriesPlot(t._3, t._5 + 3 * Math.sqrt(t._7), t._13, t._14, t._15, t._16) }

      PlotIT.plotBoth(alertFunction, "original wikitraffic", "alert function", page, outputPath)
    }
  }

}
