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

  def plot(pageFile : String) = {

    implicit val env = ExecutionEnvironment.getExecutionEnvironment

    val data = WikiUtils.readWikiTrafficCSV(pageFile)

    plotData(data)
  }

}