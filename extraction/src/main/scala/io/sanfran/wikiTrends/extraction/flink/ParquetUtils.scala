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

import io.sanfran.wikiTrends.commons.WikiTrafficThrift
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.hadoop.mapreduce._
import org.apache.hadoop.fs.Path

import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import parquet.hadoop.ParquetOutputFormat
import parquet.hadoop.metadata.CompressionCodecName
import parquet.hadoop.thrift.{ParquetThriftInputFormat, ParquetThriftOutputFormat}

object ParquetUtils {

  def writeParquet(dataOut: DataSet[WikiTrafficID], outputPath: String) {
    
    val data = dataOut.map {t => 
      new Tuple2[Void,WikiTrafficThrift](null,
        new WikiTrafficThrift(t.projectName, t.pageTitle, t.requestNumber, t.contentSize, t.year, t.month, t.day, t.hour))
    }
    
    val job = Job.getInstance

    // Set up Hadoop Output Format
    val parquetFormat =
      new HadoopOutputFormat[Void, WikiTrafficThrift](new ParquetThriftOutputFormat, job)

    FileOutputFormat.setOutputPath(job, new Path(outputPath))

    ParquetThriftOutputFormat.setThriftClass(job, classOf[WikiTrafficThrift])
    ParquetOutputFormat.setCompression(job, CompressionCodecName.SNAPPY)
    ParquetOutputFormat.setEnableDictionary(job, true)

    // Output & Execute
    data.output(parquetFormat)
  }

  def readParquet(env: ExecutionEnvironment, inputPath: String): DataSet[WikiTrafficID] = {
    val job = Job.getInstance

    val parquetFormat = new HadoopInputFormat[Void, WikiTrafficThrift](new ParquetThriftInputFormat(), classOf[Void],
      classOf[WikiTrafficThrift], job)

    FileInputFormat.addInputPath(job, new Path(inputPath))
    
    val data = env.createInput(parquetFormat).map { t => 
      new WikiTrafficID(t._2.projectName, t._2.pageTitle, t._2.requestNumber, t._2.contentSize, t._2.year, t._2.month, t._2.day, t._2.hour)}

    return data
  }


}
