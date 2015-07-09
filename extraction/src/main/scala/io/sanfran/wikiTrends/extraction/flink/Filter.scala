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
import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.core.fs.FileSystem.WriteMode

object Filter extends App {

  override def main(args: Array[String]) {
    super.main(args)
    histo(args(0), args(1), args(2))
  }
  
  def histo(pageFile : String, outputPath: String, whiteList: String) = {

    implicit val env = ExecutionEnvironment.getExecutionEnvironment

    val white = env.readCsvFile[Tuple2[String,String]](whiteList, fieldDelimiter = " ")
                 
    val data = WikiUtils.readWikiTrafficTuple(pageFile)
    
    val filtered = data.joinWithTiny(white).where(0,1).equalTo(0,1) { (a,_) => a }
           
    filtered.writeAsCsv(outputPath + "filtered", writeMode = WriteMode.OVERWRITE, fieldDelimiter = " ")
    
    env.execute()
    
  }

}
