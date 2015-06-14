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

import java.util.Date

import io.sanfran.wikiTrends.extraction.WikiUtils
import org.apache.commons.io.FileUtils
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala.DataSet
import org.apache.flink.api.scala._

import java.net.URL
import java.io.File


object Downloader extends App {
  
  download()

  def download() = {

    val batchSize: Int = 10
    val startDate: Date = new Date(2008, 0, 1)
    val endDate: Date = new Date(2015, 0, 1)

    implicit val env = ExecutionEnvironment.getExecutionEnvironment

    var count = 0L

    var d = startDate
    do {
      val day = d.getDate
      val month = d.getMonth
      val year = d.getYear

      val dayS = String.format("%02d", day: Integer)
      val monthS = String.format("%02d", (d.getMonth + 1): Integer)

      for (hour <- 0 to 23) {
        val hourS = String.format("%02d", hour: Integer)

        var found: Boolean = false
        var version = 0
        while (!found) {
          val versionS = String.format("%04d", version: Integer)

          val file = "pagecounts-" + year + monthS + dayS + "-" + hourS + versionS + ".gz"
          val url = "http://dumps.wikimedia.org/other/pagecounts-raw/" + year + "/" + year + "-" + monthS + "/" + file

          println(file)

          try {
            val fileF = new File("/tmp/new/" + file)
            val urlU = new URL(url)
            FileUtils.copyURLToFile(urlU, fileF)

            found = true
            count = count + 1L
          } catch {
            case e: Exception => println(e)
          }

          version = version + 1
          if (version == 10) {
            found = true
          }
        }
        //run flink job after each batch of 10 files
        if (count % batchSize == 0) {
          println("starting flink")
          val traffic = WikiUtils.readWikiTrafficID("/tmp/new/")
          //val filtered = traffic.filter { t => t.pageTitle.equals("Barack_Obama") && t.projectName.equals("en") }

          //TODO: join with white list
          val filtered = traffic.filter { t => t.projectName.equals("en") }

          //ParquetUtils.writeParquet(filtered, "/tmp/resultWiki/csv" + (count / batchSize))

          env.execute()

          val files = new File("/tmp/new/").listFiles()
          for (f <- files) {
            f.delete()
          }
        }
      }
      d = DateUtils.addDays(d, 1)
    } while (d.before(endDate))
  }

}
