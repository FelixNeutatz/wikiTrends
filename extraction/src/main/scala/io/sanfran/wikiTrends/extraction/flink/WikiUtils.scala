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

package io.sanfran.wikiTrends.extraction

import io.sanfran.wikiTrends.extraction.flink.{WikiTrafficID, WikiTraffic, Time}
import io.sanfran.wikiTrends.extraction.hadoop.FileNameTextInputFormat
import org.apache.flink.api.scala._
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{Text, LongWritable}
import org.apache.hadoop.mapred.{FileInputFormat, TextInputFormat, JobConf}


object WikiUtils {

  def readWikiTrafficID(file: String)(implicit env: ExecutionEnvironment) = {

    val job = new JobConf()
    val hadoopInput = new FileNameTextInputFormat()
    FileInputFormat.addInputPath(job, new Path(file))
    val lines = env.createHadoopInput(hadoopInput, classOf[Text], classOf[Text], job)

    lines.map { line =>
      val columns = line._2.toString.split(" ")
      
      val time = parseTime(line._1.toString)
      
      new WikiTrafficID(columns(0), columns(1), columns(2).toLong, columns(3).toLong, time.year, time.month, time.day, time.hour) }
  }

  def readWikiTrafficCSV(file: String)(implicit env: ExecutionEnvironment) = {

    val job = new JobConf()
    val hadoopInput = new TextInputFormat()
    FileInputFormat.addInputPath(job, new Path(file))
    val lines = env.createHadoopInput(hadoopInput, classOf[LongWritable], classOf[Text], job)

    lines.map { line =>
      val columns = line._2.toString.split("\t")
      
      new WikiTrafficID(columns(0), columns(1), columns(2).toLong, columns(3).toLong, columns(4).toShort, columns(5).toByte, columns(6).toByte, columns(7).toByte) }
  }

  def readWikiTraffic(file: String)(implicit env: ExecutionEnvironment) = {
    
    val time = parseTime(file)

    val job = new JobConf()
    val hadoopInput = new TextInputFormat()
    FileInputFormat.addInputPath(job, new Path(file))
    val lines = env.createHadoopInput(hadoopInput, classOf[LongWritable], classOf[Text], job)
    
    lines.map { line => 
      val columns = line._2.toString.split(" ")
      new WikiTraffic(columns(0), columns(1), columns(2).toLong, columns(3).toLong, time) }    
  }
  
  def parseTime(file: String) = {
    val startFilename = file.lastIndexOf("/") + 1
    
    val year = file.substring(startFilename + 11, startFilename + 15).toShort
    val month = file.substring(startFilename + 15, startFilename + 17).toByte
    val day = file.substring(startFilename + 17, startFilename + 19).toByte
    val hour = file.substring(startFilename + 20, startFilename + 22).toByte
    
    new Time(year, month, day, hour)
  }

}
