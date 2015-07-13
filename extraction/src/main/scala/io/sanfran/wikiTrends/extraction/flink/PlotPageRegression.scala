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

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.DataSet
import org.apache.flink.core.fs.FileSystem.WriteMode

object PlotPageRegression extends App {

  override def main(args: Array[String]) {
    super.main(args)
    plotPage(args(0), args(1), args(2), args(3), args(4).toBoolean)
  } 

  def plotPage(inputPath: String, projectName: String, page: String, outputPath: String, generatePlots: Boolean) = {

    implicit val env = ExecutionEnvironment.getExecutionEnvironment    

    val data = WikiUtils.readWikiTrafficCSV(inputPath, " ").filter( t => t.projectName.equals(projectName) && t.pageTitle.equals(page))
    
    if (data.count() == 0) {
      throw new Exception("Page not found")
    }
    
    val result = Regression.applyRegression(data)

    val diff = result._1
    val threshold = result._2
    
    if (!generatePlots) {
      result._1.map { t => (t._1, t._2, t._3, t._4, t._5, t._6, t._7, threshold) }.writeAsCsv(outputPath + "plot", writeMode = WriteMode.OVERWRITE, fieldDelimiter = " ")
      env.execute()
    } else {

      val model = diff.map { t => TwoSeriesPlot(t._1, t._2, t._4, t._5, t._6, t._7) }

      PlotIT.plotBoth(model, "original wikitraffic", "regression model", page, outputPath)

      val diffWithThreshold = diff.map { t => TwoSeriesPlot(t._3, threshold, t._4, t._5, t._6, t._7) }

      PlotIT.plotBoth(diffWithThreshold, "Difference: original wikitraffic - regression model", "threshold", page, outputPath)
      

      val alertFunction = diff.map { t => TwoSeriesPlot(t._1, t._2 + threshold, t._4, t._5, t._6, t._7) }

      PlotIT.plotBoth(alertFunction, "original wikitraffic", "alert function", page, outputPath)
    }
  }

}
