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

import io.sanfran.wikiTrends.Config
import io.sanfran.wikiTrends.extraction.WikiUtils
import io.sanfran.wikiTrends.extraction.plots.PlotTimeSeries
import org.apache.flink.api.scala.{ExecutionEnvironment, DataSet}
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
  
  def plotDiffWithThreshold(data: DataSet[DataHourIdTime], threshold: Double): Unit = {
    val dataLocal = data.collect().toList
    val s: TimeSeries = new TimeSeries("Wikipedia Traffic difference", classOf[Hour])
    val s2: TimeSeries = new TimeSeries("Quantile threshold", classOf[Hour])
    for (t <- dataLocal) {
      s.add(new Hour(t.hour, t.day, t.month, t.year), t.visits)
      s2.add(new Hour(t.hour, t.day, t.month, t.year), threshold)
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
