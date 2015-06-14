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

import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.io.compress.SplitCompressionInputStream;
import org.apache.hadoop.io.compress.SplittableCompressionCodec;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;
import org.apache.hadoop.mapred.*;

public class FileNameLineRecordReader implements RecordReader<Text, Text> {
		private static final Log LOG
				= LogFactory.getLog(FileNameLineRecordReader.class.getName());

		private CompressionCodecFactory compressionCodecs = null;
		private long start;
		private long pos;
		private long end;
		private LineReader in;
		int maxLineLength;
		private Seekable filePosition;
		private CompressionCodec codec;
		private Decompressor decompressor;
	  private String fileName;

		/**
		 * A class that provides a line reader from an input stream.
		 * @deprecated Use {@link org.apache.hadoop.util.LineReader} instead.
		 */
		@Deprecated
		public static class LineReader extends org.apache.hadoop.util.LineReader {
			LineReader(InputStream in) {
				super(in);
			}
			LineReader(InputStream in, int bufferSize) {
				super(in, bufferSize);
			}
			public LineReader(InputStream in, Configuration conf) throws IOException {
				super(in, conf);
			}
		}

		public FileNameLineRecordReader(Configuration job,
														FileSplit split) throws IOException {
			this.maxLineLength = job.getInt("mapred.LineRecordReader.maxlength",
					Integer.MAX_VALUE);
			fileName = split.getPath().getName();
			start = split.getStart();
			end = start + split.getLength();
			final Path file = split.getPath();
			compressionCodecs = new CompressionCodecFactory(job);
			codec = compressionCodecs.getCodec(file);

			// open the file and seek to the start of the split
			FileSystem fs = file.getFileSystem(job);
			FSDataInputStream fileIn = fs.open(split.getPath());

			if (isCompressedInput()) {
				decompressor = CodecPool.getDecompressor(codec);
				if (codec instanceof SplittableCompressionCodec) {
					final SplitCompressionInputStream cIn =
							((SplittableCompressionCodec)codec).createInputStream(
									fileIn, decompressor, start, end,
									SplittableCompressionCodec.READ_MODE.BYBLOCK);
					in = new LineReader(cIn, job);
					start = cIn.getAdjustedStart();
					end = cIn.getAdjustedEnd();
					filePosition = cIn; // take pos from compressed stream
				} else {
					in = new LineReader(codec.createInputStream(fileIn, decompressor), job);
					filePosition = fileIn;
				}
			} else {
				fileIn.seek(start);
				in = new LineReader(fileIn, job);
				filePosition = fileIn;
			}
			// If this is not the first split, we always throw away first record
			// because we always (except the last split) read one extra line in
			// next() method.
			if (start != 0) {
				start += in.readLine(new Text(), 0, maxBytesToConsume(start));
			}
			this.pos = start;
		}

		private boolean isCompressedInput() {
			return (codec != null);
		}

		private int maxBytesToConsume(long pos) {
			return isCompressedInput()
					? Integer.MAX_VALUE
					: (int) Math.min(Integer.MAX_VALUE, end - pos);
		}

		private long getFilePosition() throws IOException {
			long retVal;
			if (isCompressedInput() && null != filePosition) {
				retVal = filePosition.getPos();
			} else {
				retVal = pos;
			}
			return retVal;
		}

		public FileNameLineRecordReader(InputStream in, long offset, long endOffset,
														int maxLineLength) {
			this.maxLineLength = maxLineLength;
			this.in = new LineReader(in);
			this.start = offset;
			this.pos = offset;
			this.end = endOffset;
			this.filePosition = null;
		}

		public FileNameLineRecordReader(InputStream in, long offset, long endOffset,
														Configuration job)
				throws IOException{
			this.maxLineLength = job.getInt("mapred.LineRecordReader.maxlength",
					Integer.MAX_VALUE);
			this.in = new LineReader(in, job);
			this.start = offset;
			this.pos = offset;
			this.end = endOffset;
			this.filePosition = null;
		}

		public Text createKey() {
			return new Text();
		}

		public Text createValue() {
			return new Text();
		}

		/** Read a line. */
		public synchronized boolean next(Text key, Text value)
				throws IOException {

			// We always read one extra line, which lies outside the upper
			// split limit i.e. (end - 1)
			while (getFilePosition() <= end) {
				key.set(fileName);

				int newSize = in.readLine(value, maxLineLength,
						Math.max(maxBytesToConsume(pos), maxLineLength));
				if (newSize == 0) {
					return false;
				}
				pos += newSize;
				if (newSize < maxLineLength) {
					return true;
				}

				// line too long. try again
				LOG.info("Skipped line of size " + newSize + " at pos " + (pos - newSize));
			}

			return false;
		}

		/**
		 * Get the progress within the split
		 */
		public float getProgress() throws IOException {
			if (start == end) {
				return 0.0f;
			} else {
				return Math.min(1.0f,
						(getFilePosition() - start) / (float)(end - start));
			}
		}

		public synchronized long getPos() throws IOException {
			return pos;
		}

		public synchronized void close() throws IOException {
			try {
				if (in != null) {
					in.close();
				}
			} finally {
				if (decompressor != null) {
					CodecPool.returnDecompressor(decompressor);
				}
			}
		}
	}
