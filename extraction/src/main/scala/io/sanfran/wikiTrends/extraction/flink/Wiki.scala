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

case class Time(year: Short, month: Short, day: Short, hour: Short)

case class WikiTraffic(projectName: String, pageTitle: String, requestNumber: Long, contentSize: Long, time: Time)

case class WikiTrafficID(projectName: String, pageTitle: String, requestNumber: Long, contentSize: Long, year: Short, month: Short, day: Short, hour: Short)

case class RegressionData(y: Double, oneHourAgo: Double, twoHoursAgo: Double, threeHoursAgo: Double, twentyFourHoursAgo: Double, fourtyEightHoursAgo : Double)
