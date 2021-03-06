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
import java.util.Calendar
import java.util.concurrent.TimeUnit

object DateUtils {

  def addDays(date: Date, days: Integer): Date = {
    val cal = Calendar.getInstance()
    cal.setTime(date)
    cal.add(Calendar.DATE, days)
    return cal.getTime()
  }

  def addHours(date: Date, hours: Integer): Date = {
    val cal = Calendar.getInstance()
    cal.setTime(date)
    cal.add(Calendar.HOUR, hours)
    return cal.getTime()
  }

  def diffDays(date1: Date, date2: Date) : Long = {
    val diff = date2.getTime() - date1.getTime();
    return TimeUnit.DAYS.convert(diff, TimeUnit.MILLISECONDS)
  }

  def diffHours(date1: Date, date2: Date) : Long = {
    val diff = date2.getTime() - date1.getTime();
    return TimeUnit.HOURS.convert(diff, TimeUnit.MILLISECONDS)
  }
}
