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
import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.scala.ExecutionEnvironment


object TopAnomalies extends App {

  override def main(args: Array[String]) {
    super.main(args)
    topKAnomalies(args(0), args(1).toInt)
  }

  def topKAnomalies(pageFile : String, k: Int) = {

    implicit val env = ExecutionEnvironment.getExecutionEnvironment
    
    val anomalies = WikiUtils.readAnomCSVTuple(pageFile)
    
    /*
    val topK = anomalies.groupBy(2,3,4)
        .sortGroup(6, Order.DESCENDING)
        .first(k)
        .distinct(0,1)
        .collect()*/
    
    val topK = anomalies.collect()
    
    topK.sortWith(_._7 > _._7).foreach(println)
    
  }

}
