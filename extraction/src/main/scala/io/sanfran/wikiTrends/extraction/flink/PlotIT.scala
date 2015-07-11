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

import java.io.File

import io.sanfran.wikiTrends.Config
import io.sanfran.wikiTrends.extraction.WikiUtils
import io.sanfran.wikiTrends.extraction.plots.PlotTimeSeries
import org.apache.flink.api.scala.{ExecutionEnvironment, DataSet}
import org.jfree.chart.ChartUtilities
import org.jfree.data.time.{TimeSeriesCollection, Hour, TimeSeries}
import org.jfree.ui.RefineryUtilities

object PlotIT extends App {
  

  plot(Config.get("Obama.sample.path"))

  def plotData(data: DataSet[WikiTrafficID]): Unit = {
    val dataLocal = data.collect().toList

    val s: TimeSeries = new TimeSeries("Wikipedia Traffic Obama page", classOf[Hour])
    for (t <- dataLocal) {
      s.add(new Hour(t.hour, t.day, t.month, t.year), t.requestNumber)
    }

    val dataset: TimeSeriesCollection = new TimeSeriesCollection
    dataset.addSeries(s)
    dataset.setDomainIsPointsInTime(true)

    val demo = new PlotTimeSeries("Wikipedia Traffic", dataset)
    demo.pack()
    RefineryUtilities.centerFrameOnScreen(demo)
    demo.setVisible(true)
  }

  def plotDataPredictions(data: DataSet[DataHourIdTime]): Unit = {
    val dataLocal = data.collect().toList

    val s: TimeSeries = new TimeSeries("Wikipedia Traffic Obama page", classOf[Hour])
    for (t <- dataLocal) {
      s.add(new Hour(t.hour, t.day, t.month, t.year), t.visits)
    }

    val dataset: TimeSeriesCollection = new TimeSeriesCollection
    dataset.addSeries(s)
    dataset.setDomainIsPointsInTime(true)

    val demo = new PlotTimeSeries("Wikipedia Traffic", dataset)
    demo.pack()
    RefineryUtilities.centerFrameOnScreen(demo)
    demo.setVisible(true)
  }
  
  def plotDiffWithThreshold(data: DataSet[(Double, Double, Double, Short, Short, Short, Short)], threshold: Double, page: String): Unit = {
    val dataLocal = data.collect().toList
    val s: TimeSeries = new TimeSeries("Wikipedia Traffic difference for page: " + page, classOf[Hour])
    val s2: TimeSeries = new TimeSeries("threshold", classOf[Hour])
    for (t <- dataLocal) {
      s.add(new Hour(t._7, t._6, t._5, t._4), t._3)
      s2.add(new Hour(t._7, t._6, t._5, t._4), threshold)
    }

    val dataset: TimeSeriesCollection = new TimeSeriesCollection
    dataset.addSeries(s)
    dataset.addSeries(s2)
    //dataset.setDomainIsPointsInTime(true)

    val demo = new PlotTimeSeries("Wikipedia Traffic", dataset)
    demo.pack()
    RefineryUtilities.centerFrameOnScreen(demo)
    demo.setVisible(true)
  }

  def plotBoth(data: DataSet[TwoSeriesPlot], series1Name: String, series2Name: String, page: String, output: String = null): Unit = {
    val dataLocal = data.collect().toList
    val s: TimeSeries = new TimeSeries(series1Name, classOf[Hour])
    val s2: TimeSeries = new TimeSeries(series2Name, classOf[Hour])
    for (t <- dataLocal) {
      s.addOrUpdate(new Hour(t.hour, t.day, t.month, t.year), t.series1)
      s2.addOrUpdate(new Hour(t.hour, t.day, t.month, t.year), t.series2)
    }

    val dataset: TimeSeriesCollection = new TimeSeriesCollection
    dataset.addSeries(s)
    dataset.addSeries(s2)
    //dataset.setDomainIsPointsInTime(true)

    val demo = new PlotTimeSeries("Wikipedia Traffic - \""+ page + "\"", dataset)
    
    if (output != null) {
      ChartUtilities.saveChartAsPNG(new File(output + page + "_" + series1Name + " - " + series2Name), demo.getJFreeChart, 1120, 700)
    } else{
      demo.pack()
      RefineryUtilities.centerFrameOnScreen(demo)
      demo.setVisible(true)
    }
  }

  def plotBoth(data: DataSet[DataHourIdTime]): Unit = {
    val dataLocal = data.collect().toList
    val s: TimeSeries = new TimeSeries("Wikipedia Traffic original", classOf[Hour])
    val s2: TimeSeries = new TimeSeries("Wikipedia Traffic prediction", classOf[Hour])
    for (t <- dataLocal) {
      s.add(new Hour(t.hour, t.day, t.month, t.year), t.orginalVisits)
      s2.add(new Hour(t.hour, t.day, t.month, t.year), t.visits)
    }

    val dataset: TimeSeriesCollection = new TimeSeriesCollection
    dataset.addSeries(s)
    dataset.addSeries(s2)
    //dataset.setDomainIsPointsInTime(true)

    val demo = new PlotTimeSeries("Wikipedia Traffic", dataset)
    demo.pack()
    RefineryUtilities.centerFrameOnScreen(demo)
    demo.setVisible(true)
  }

  def plot(pageFile : String) = {

    implicit val env = ExecutionEnvironment.getExecutionEnvironment

    val data = WikiUtils.readWikiTrafficCSV(pageFile, "\t")

    plotData(data)
  }

}
