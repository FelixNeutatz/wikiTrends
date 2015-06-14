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

package io.sanfran.wikiTrends.extraction.hadoop;

import java.io.*;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.*;
import org.apache.hadoop.mapred.*;

/** An {@link InputFormat} for plain text files.  Files are broken into lines.
 * Either linefeed or carriage-return are used to signal end of line.  Keys are
 * the position in the file, and values are the line of text.. 
 */
public class FileNameTextInputFormat extends FileInputFormat<Text, Text>
		implements JobConfigurable {

	private CompressionCodecFactory compressionCodecs = null;

	public void configure(JobConf conf) {
		compressionCodecs = new CompressionCodecFactory(conf);
	}

	protected boolean isSplitable(FileSystem fs, Path file) {
		final CompressionCodec codec = compressionCodecs.getCodec(file);
		if (null == codec) {
			return true;
		}
		return codec instanceof SplittableCompressionCodec;
	}

	public RecordReader<Text, Text> getRecordReader(
			InputSplit genericSplit, JobConf job,
			Reporter reporter)
			throws IOException {

		reporter.setStatus(genericSplit.toString());
		return new FileNameLineRecordReader(job, (FileSplit) genericSplit);
	}
}
