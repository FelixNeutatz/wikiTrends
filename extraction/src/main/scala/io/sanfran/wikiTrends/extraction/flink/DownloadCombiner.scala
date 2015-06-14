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
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala.DataSet
import org.apache.flink.api.scala._

object DownloadCombiner extends App {

  combine()

  def combine() = {

    implicit val env = ExecutionEnvironment.getExecutionEnvironment

    val folder = "/share/flink/data/"
    //val folder = "/tmp/"

    var begin = true
    val file = new File(folder  + "resultWiki")

    var data: DataSet[WikiTrafficID] = null    
    
    for(name <- file.list())
    {
      if (new File(folder + "resultWiki/" + name).isDirectory())
      {
        if (begin) {
          data = ParquetUtils.readParquet(env, "file://" + folder + "resultWiki/" + name)
          begin = false
        } else {
          data = data.union(ParquetUtils.readParquet(env, "file://" + folder + "resultWiki/" + name))
        }
      }
    }
    
    println(data.count)
    
    ParquetUtils.writeParquet(data, "file://" + folder + "finalResultWiki/")
    
    env.execute()    
  }

}
