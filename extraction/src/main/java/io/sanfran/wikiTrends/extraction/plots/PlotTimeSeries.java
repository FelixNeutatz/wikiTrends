package io.sanfran.wikiTrends.extraction.plots;

import java.awt.Color;
import java.text.SimpleDateFormat;

import javax.swing.JPanel;

import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartPanel;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.axis.DateAxis;
import org.jfree.chart.plot.XYPlot;
import org.jfree.chart.renderer.xy.XYItemRenderer;
import org.jfree.chart.renderer.xy.XYLineAndShapeRenderer;
import org.jfree.data.time.TimeSeriesCollection;
import org.jfree.data.xy.XYDataset;
import org.jfree.ui.ApplicationFrame;
import org.jfree.ui.RectangleInsets;

public class PlotTimeSeries extends ApplicationFrame {

  public PlotTimeSeries(String title, TimeSeriesCollection data) {
    super(title);
    ChartPanel chartPanel = (ChartPanel) createPanel(data, title);
    chartPanel.setPreferredSize(new java.awt.Dimension(1024, 768));
    chartPanel.setMouseZoomable(true, false);
    setContentPane(chartPanel);
  }

  private static JFreeChart createChart(XYDataset dataset, String title) {

    JFreeChart chart = ChartFactory.createTimeSeriesChart(
        title,  // title
        "Date",             // x-axis label
        "Page visits",   // y-axis label
        dataset,            // data
        true,               // create legend?
        true,               // generate tooltips?
        false               // generate URLs?
    );

    chart.setBackgroundPaint(Color.white);

    XYPlot plot = (XYPlot) chart.getPlot();
    plot.setBackgroundPaint(Color.lightGray);
    plot.setDomainGridlinePaint(Color.white);
    plot.setRangeGridlinePaint(Color.white);
    plot.setAxisOffset(new RectangleInsets(5.0, 5.0, 5.0, 5.0));
    plot.setDomainCrosshairVisible(true);
    plot.setRangeCrosshairVisible(true);

    XYItemRenderer r = plot.getRenderer();
    if (r instanceof XYLineAndShapeRenderer) {
      XYLineAndShapeRenderer renderer = (XYLineAndShapeRenderer) r;
      renderer.setBaseShapesVisible(false); // if you want visible dots -> true
      renderer.setBaseShapesFilled(false); // if you want visible dots -> true
    }

    DateAxis axis = (DateAxis) plot.getDomainAxis();
    //axis.setDateFormatOverride(new SimpleDateFormat("MMM-yyyy"));
    axis.setDateFormatOverride(new SimpleDateFormat("dd-MM-yyyy"));

    return chart;

  }

  public static JPanel createPanel(TimeSeriesCollection data, String title) {
    JFreeChart chart = createChart(data, title);
    return new ChartPanel(chart);
  }

}
