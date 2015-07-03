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

import java.text.DecimalFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Random;

import org.apache.commons.io.FileUtils;

import java.net.URL;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;


public class Downloader {
	
  private static final String DOWNLOAD_FOLDER = "/home/alanizkupsch/data/";
  
  public static void download() throws IOException {
	  
	int fileId = new Random().nextInt();
	
	FileWriter fileStats = new FileWriter(DOWNLOAD_FOLDER + "fileDownloadStatistics2014_" + fileId + ".txt");
	fileStats.write("File Size(in Byte) Time(in s) Speed(in MB/s) Date\n");
	fileStats.flush();
	FileWriter allStats = new FileWriter(DOWNLOAD_FOLDER + "sessionDownloadStatistics2014_" + fileId + ".txt");
	allStats.write("NumberOfFiles TotalFileSize(in Byte) TotalTime(in s) AverageSpeed(in MB/s) Date\n");
	allStats.flush();
	double downloadTimeInSec = 0D;
	long totalFileSize = 0L;

    Date startDate = new Date(2014, 0, 1);
    Date endDate = new Date(2015, 0, 1);

    long count = 0L;

    DecimalFormat format2 = new DecimalFormat("00");
    DecimalFormat format4 = new DecimalFormat("0000");
    Date d = startDate;
    do {
      int day = d.getDate();
      int month = d.getMonth();
      int year = d.getYear();

      String dayS = format2.format(day);
      String monthS = format2.format(month + 1);

      for (int hour = 0; hour < 24; hour++) {
        String hourS = format2.format(hour);

        boolean found = false;
        int version = 0;
        while (!found) {
          String versionS = format4.format(version);

          String file = "pagecounts-" + year + monthS + dayS + "-" + hourS + versionS + ".gz";
          String url = "http://dumps.wikimedia.org/other/pagecounts-raw/" + year + "/" + year + "-" + monthS + "/" + file;

          System.out.println(file);

          
          try {
        	File fileF = new File(DOWNLOAD_FOLDER + file);
			URL urlU = new URL(url);
			long start = System.nanoTime();
			
			//apache way
			FileUtils.copyURLToFile(urlU, fileF);
			
			//alternative way - test if faster
//			ReadableByteChannel rbc = Channels.newChannel(urlU.openStream());
//			FileOutputStream fos = new FileOutputStream(fileF);
//			fos.getChannel().transferFrom(rbc, 0, Long.MAX_VALUE);
//			fos.close();
			
			found = true;
			count = count + 1L;
			
			long fileSize = FileUtils.sizeOf(fileF);
			double timeSec = ((double)System.nanoTime() - start) / 1000000000D;
			String currDate = new Date().toString();
			downloadTimeInSec += timeSec;
			totalFileSize += fileSize;
			
			fileStats.write(file + " " + fileSize + " " + timeSec + " " + (double)fileSize / 1000000D / timeSec + " '" + currDate + "'\n");
			fileStats.flush();
			allStats.write(count + " " + totalFileSize + " " + downloadTimeInSec + " " + (double)totalFileSize / 1000000D / downloadTimeInSec + " '" + currDate + "'\n");
			allStats.flush();
          } catch (Exception e) {
        	//do nothing - go to next possible file
        	//e.printStackTrace();
          }

          version = version + 1;
          if (version == 10) {
            found = true;
          }
        }
      }
      d = addDays(d, 1);
    } while (d.before(endDate));
    
    fileStats.close();
    allStats.close();
  }
  
  private static Date addDays(Date date, int days) {
    Calendar cal = Calendar.getInstance();
    cal.setTime(date);
    cal.add(Calendar.DATE, days);
    return cal.getTime();
  }
  
  public static void main(String[] args) {
	  try {
		Downloader.download();
	} catch (IOException e) {
		e.printStackTrace();
	}
  }

}
