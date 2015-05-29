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

package io.sanfran.wikiTrends;

import java.util.Properties;

public class Config {

  private static Properties props;

  private Config() {}

  public static String get(String key) {
    try {
      if (props == null) {
        props = new Properties();
        props.load(Config.class.getResourceAsStream("/conf/conf.properties"));
        validateConfig();
      }
      return props.getProperty(key);
    } catch (Exception e) {
      throw new IllegalStateException("Unable to load config file!", e);
    }
  }

  private static void validateConfig() {

    if (nullOrEmpty("wikitraffic.samples.path")) {
      throw new IllegalStateException("[wikitraffic.samples.path] in conf.properties must point to a compressed wiki traffic file like " +
          "pagecounts-iiiiiiii-iiiiii.gz");
    }
  }

  private static boolean nullOrEmpty(String key) {
    return props.getProperty(key) == null || "".equals(props.getProperty(key));
  }

  private static boolean endsWith(String key, String pattern) {
    return props.getProperty(key).endsWith(pattern);
  }
}
