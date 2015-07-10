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

import com.esotericsoftware.kryo.io.{Output, Input}
import com.esotericsoftware.kryo.{Kryo, Serializer}
import com.tdunning.math.stats.ArrayDigest
import io.sanfran.wikiTrends.Config
import io.sanfran.wikiTrends.extraction.WikiUtils

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.configuration.Configuration
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.ml.common.LabeledVector
import org.apache.flink.ml.math.DenseVector
import org.apache.flink.ml.regression.MultipleLinearRegression
import org.apache.flink.ml._

import org.joda.time._

import org.apache.flink.api.scala._
import org.apache.flink.api.scala.DataSet


object AnomaliesPerDays extends App {

  override def main(args: Array[String]) {
    super.main(args)
    newsletter(args(0))
  }

  def newsletter(pageFile : String) = {

    implicit val env = ExecutionEnvironment.getExecutionEnvironment
    
    val anomalies = WikiUtils.readAnomCSVTuple(pageFile)
    
    anomalies.groupBy(2,3,4)
        .sortGroup(6, Order.DESCENDING)
        .first(5)
        .print
    
  }

}
