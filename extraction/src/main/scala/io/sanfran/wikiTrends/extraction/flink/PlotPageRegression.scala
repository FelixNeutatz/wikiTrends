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
import java.awt.Color

object PlotPageRegression extends App {

  override def main(args: Array[String]) {
    super.main(args)
    plotPage(args(0), args(1), args(2), args(3), args(4).toBoolean, args(5))
  }

  def plotPage(pageFile : String, projectName: String, page: String, outputPath: String, generatePlots: Boolean, title: String) = {

    implicit val env = ExecutionEnvironment.getExecutionEnvironment


    val data = WikiUtils.readWikiTrafficCSV(pageFile, " ").filter( t => t.projectName.equals(projectName) && t.pageTitle.equals(page))

    if (data.count() == 0) {
      throw new Exception("Page not found")
    }
    
    if(!generatePlots) {
      data.writeAsCsv(outputPath + "csv_plot_filter", fieldDelimiter = " ")
      env.execute()
    } else {
      val result = Regression.applyRegression(data, false)

      val diff = result._1
      val threshold = result._2

      val model = diff.map { t => TwoSeriesPlot(t._2, t._1, t._4, t._5, t._6, t._7) }
      PlotIT.plotBoth(model, ("regression model", Color.orange, 1.0), ("original traffic", Color.black, 2.0), title, outputPath)

      val diffWithThreshold = diff.map { t => TwoSeriesPlot(t._3, threshold, t._4, t._5, t._6, t._7) }
      PlotIT.plotBoth(diffWithThreshold, ("residuals", Color.blue, 2.0), ("anomaly threshold", Color.red, 2.0), title, outputPath)
      
      val alertFunction = diff.map { t => TwoSeriesPlot(t._1, t._2 + threshold, t._4, t._5, t._6, t._7) }
      PlotIT.plotBoth(alertFunction, ("original traffic", Color.black, 2.0), ("anomaly threshold", Color.red, 2.0), title, outputPath)


      val model1 = diff.map { t => ThreeSeriesPlot(t._2, t._2 + threshold, t._1, t._4, t._5, t._6, t._7) }
      PlotIT.plotThree(model1, ("regression model", Color.orange, 1.0), ("anomaly threshold", Color.red, 2.0), ("original traffic", Color.black, 2.0), title, outputPath)
    }
  }

}
