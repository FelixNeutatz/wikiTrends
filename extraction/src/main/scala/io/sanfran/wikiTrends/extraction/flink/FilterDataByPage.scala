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
import io.sanfran.wikiTrends.Config
import org.apache.flink.api.scala._


object FilterDataByPage extends App {

  filterByPage(Config.get("Obama.sample.path"))

  def filterByPage(wikiFile: String) = {

    implicit val env = ExecutionEnvironment.getExecutionEnvironment

    val traffic = WikiUtils.readWikiTrafficID(wikiFile)
  }

}
