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

import java.io.File
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala.DataSet
import org.apache.flink.api.scala._

object DownloadCombiner extends App {

  combine()

  def combine() = {

    implicit val env = ExecutionEnvironment.getExecutionEnvironment

    var begin = true
    val file = new File("/tmp/resultWiki")

    var data: DataSet[WikiTrafficID] = null    
    
    for(name <- file.list())
    {
      if (new File("/tmp/resultWiki/" + name).isDirectory())
      {
        if (begin) {
          data = ParquetUtils.readParquet(env, "/tmp/resultWiki/" + name)
          begin = false
        } else {
          data = data.union(ParquetUtils.readParquet(env, "/tmp/resultWiki/" + name))
        }
      }
    }
    ParquetUtils.writeParquet(data,"/tmp/finalResultWiki/")
    
    env.execute()    
  }

}
