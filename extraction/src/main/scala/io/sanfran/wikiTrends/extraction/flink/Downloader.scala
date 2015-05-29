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

import java.text.SimpleDateFormat

import io.sanfran.wikiTrends.extraction.WikiUtils
import org.apache.commons.io.FileUtils
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala.DataSet

import org.apache.flink.api.scala._


import java.net.URL
import java.io.{PrintWriter, File}

import org.apache.flink.core.fs.FileSystem.WriteMode

import scala.collection.mutable


object Downloader extends App {

  def dateIsValid(dateToValidate : String) : Boolean = {
    
    val dateFormat = ""

    if(dateToValidate == null){
      return false
    }

    val sdf = new SimpleDateFormat("dd/MM/yyyy")
    sdf.setLenient(false)

    try {
      //if not valid, it will throw ParseException
      val date = sdf.parse(dateToValidate)
    } catch {
      case pe: Exception => return false
    }

    return true
  }
  
  def writeCSV(list : Seq[WikiTrafficID], filename : String) = {
    val writer = new PrintWriter(filename, "UTF-8")
    for ( e <- list) {
      writer.println(e.projectName + "," + e.pageTitle + "," + e.requestNumber + "," + e.contentSize + "," + e.year + "," + e.month + "," + e.day + "," + e.hour)
    }
    writer.close()
  }

  download()

  def download() = {

    implicit val env = ExecutionEnvironment.getExecutionEnvironment
    
    var count = 0L
    
    val batchSize : Int = 10
    
    //projectName: String, pageTitle: String, requestNumber: Long, contentSize: Long, year: Short, month: Short, day: Short, hour: Short, id: Long)
    var tupleList = new mutable.MutableList[WikiTrafficID]()
    
    //start with 2007
    for (year <- 2009 until 2015) {
      for (month <- 1 until 12) {
        for (day <- 1 until 31) {
          val dayS = String.format("%02d", day: Integer)
          val monthS = String.format("%02d", month: Integer)
          
          if (dateIsValid(dayS + "/" + monthS + "/" + year)) {
            for (hour <- 0 until 23) {
              val hourS = String.format("%02d", hour: Integer)
              
              var found : Boolean = false
              var version = 0
              while(!found) {                
                val versionS = String.format("%04d", version: Integer)

                val file = "pagecounts-" + year + monthS + dayS + "-" + hourS + versionS + ".gz"
                val url = "http://dumps.wikimedia.org/other/pagecounts-raw/" + year + "/" + year + "-" + monthS + "/" + file
                
                try {
                  val fileF = new File("/tmp/new/" + file)
                  val urlU = new URL(url)
                  FileUtils.copyURLToFile(urlU,fileF)
                  
                  found = true
                  count = count + 1L
                } catch {
                  case e: Exception => println(e)
                }
                
                version = version + 1
              }
              //run flink job after each batch of 10 files
              if (count % batchSize == 0) {
                val traffic = WikiUtils.readWikiTrafficID("/tmp/new/")
                val filtered = traffic.filter { t => t.pageTitle.equals("Barack_Obama") && t.projectName.equals("en") }

                filtered.writeAsCsv("/tmp/resultWiki/csv" + (count / batchSize), fieldDelimiter = "\t", writeMode = WriteMode.OVERWRITE)

                val files = new File("/tmp/new/").listFiles()
                for (f <- files) {
                  f.delete()
                }
              }
            }
          }
        }
      }
    }
  }

}
