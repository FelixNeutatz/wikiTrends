/**
 * Track the trackers
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

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.ml.common.LabeledVector
import org.apache.flink.ml.math.DenseVector
import org.apache.flink.ml.regression.MultipleLinearRegression

import org.apache.flink.api.scala._
import org.apache.flink.api.scala.DataSet

object Regression extends App {

  model("/home/felix/Desktop/housing.data")
  // from
  // http://archive.ics.uci.edu/ml/machine-learning-databases/housing/housing.data

  case class House(CRIM: Double, ZN: Double, INDUS: Double, CHAS: Double, NOX: Double, RM: Double, AGE: Double, DIS: Double, RAD: Double, TAX: Double, PTRATIO: Double, B: Double, LSTAT: Double, MEDV: Double)

  def readHouseData(file: String)(implicit env: ExecutionEnvironment) = {
    env.readCsvFile[House](file, "\n", "\t")
  }
  
  def model(houseFile : String) = {

    implicit val env = ExecutionEnvironment.getExecutionEnvironment

    val data =  readHouseData(houseFile)
       
    val trainingDS = data.map { t => 
      val x = new Array[Double](13)
      x(0) = t.CRIM
      x(1) = t.ZN
      x(2) = t.INDUS
      x(3) = t.CHAS
      x(4) = t.NOX
      x(5) = t.RM
      x(6) = t.AGE
      x(7) = t.DIS
      x(8) = t.RAD
      x(9) = t.TAX
      x(10) = t.PTRATIO
      x(11) = t.B
      x(12) = t.LSTAT
      
      new LabeledVector(t.MEDV, new DenseVector(x))
    }
    
    trainingDS.print
    
    val testDS = data.map { t =>
      val x = new Array[Double](13)
      x(0) = t.CRIM
      x(1) = t.ZN
      x(2) = t.INDUS
      x(3) = t.CHAS
      x(4) = t.NOX
      x(5) = t.RM
      x(6) = t.AGE
      x(7) = t.DIS
      x(8) = t.RAD
      x(9) = t.TAX
      x(10) = t.PTRATIO
      x(11) = t.B
      x(12) = t.LSTAT

      new DenseVector(x)
    }

    val mlr = MultipleLinearRegression()
        .setIterations(10)
        .setStepsize(0.5)
        .setConvergenceThreshold(0.001)
                   
    
    mlr.fit(trainingDS)

    val weightList = mlr.weightsOption.get.collect()

    val a = weightList(0)
    
    val srs = mlr.squaredResidualSum(trainingDS).collect().apply(0)

    val predictions = mlr.predict(testDS)
    
    predictions.print //strange results !!!

    println("Squared error: " + srs)
    
  }

}
