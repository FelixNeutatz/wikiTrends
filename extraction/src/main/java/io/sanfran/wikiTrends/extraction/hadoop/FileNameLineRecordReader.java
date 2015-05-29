package io.sanfran.wikiTrends.extraction.hadoop;

import java.io.IOException;
import java.io.InputStream;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.LimitedPrivate;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
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
import org.apache.hadoop.io.compress.SplittableCompressionCodec.READ_MODE;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.RecordReader;

public class FileNameLineRecordReader implements RecordReader<Text, Text> {
  private static final Log LOG = LogFactory.getLog(FileNameLineRecordReader.class.getName());
  private CompressionCodecFactory compressionCodecs;
  private long start;
  private long pos;
  private long end;
  private FileNameLineRecordReader.LineReader in;
  private FSDataInputStream fileIn;
  private final Seekable filePosition;
  int maxLineLength;
  private CompressionCodec codec;
  private Decompressor decompressor;
  private String filename;

  public FileNameLineRecordReader(Configuration job, FileSplit split) throws IOException {
    this(job, split, (byte[])null);
    this.filename =  split.getPath().getName();
  }

  public FileNameLineRecordReader(Configuration job, FileSplit split, byte[] recordDelimiter) throws IOException {
    this.compressionCodecs = null;
    this.maxLineLength = job.getInt("mapreduce.input.linerecordreader.line.maxlength", 2147483647);
    this.start = split.getStart();
    this.end = this.start + split.getLength();
    Path file = split.getPath();

    this.filename =  split.getPath().getName();
    
    this.compressionCodecs = new CompressionCodecFactory(job);
    this.codec = this.compressionCodecs.getCodec(file);
    FileSystem fs = file.getFileSystem(job);
    this.fileIn = fs.open(file);
    if(this.isCompressedInput()) {
      this.decompressor = CodecPool.getDecompressor(this.codec);
      if(this.codec instanceof SplittableCompressionCodec) {
        SplitCompressionInputStream cIn = ((SplittableCompressionCodec)this.codec).createInputStream(this.fileIn, this.decompressor, this.start, this.end, READ_MODE.BYBLOCK);
        this.in = new FileNameLineRecordReader.LineReader(cIn, job, recordDelimiter);
        this.start = cIn.getAdjustedStart();
        this.end = cIn.getAdjustedEnd();
        this.filePosition = cIn;
      } else {
        this.in = new FileNameLineRecordReader.LineReader(this.codec.createInputStream(this.fileIn, this.decompressor), job, recordDelimiter);
        this.filePosition = this.fileIn;
      }
    } else {
      this.fileIn.seek(this.start);
      this.in = new FileNameLineRecordReader.LineReader(this.fileIn, job, recordDelimiter);
      this.filePosition = this.fileIn;
    }

    if(this.start != 0L) {
      this.start += (long)this.in.readLine(new Text(), 0, this.maxBytesToConsume(this.start));
    }

    this.pos = this.start;
  }

  public FileNameLineRecordReader(InputStream in, long offset, long endOffset, int maxLineLength) {
    this(in, offset, endOffset, maxLineLength, (byte[])null);
  }

  public FileNameLineRecordReader(InputStream in, long offset, long endOffset, int maxLineLength, byte[] recordDelimiter) {
    this.compressionCodecs = null;
    this.maxLineLength = maxLineLength;
    this.in = new FileNameLineRecordReader.LineReader(in, recordDelimiter);
    this.start = offset;
    this.pos = offset;
    this.end = endOffset;
    this.filePosition = null;
  }

  public FileNameLineRecordReader(InputStream in, long offset, long endOffset, Configuration job) throws IOException {
    this(in, offset, endOffset, job, (byte[])null);
  }

  public FileNameLineRecordReader(InputStream in, long offset, long endOffset, Configuration job, byte[] recordDelimiter) throws IOException {
    this.compressionCodecs = null;
    this.maxLineLength = job.getInt("mapreduce.input.linerecordreader.line.maxlength", 2147483647);
    this.in = new FileNameLineRecordReader.LineReader(in, job, recordDelimiter);
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

  private boolean isCompressedInput() {
    return this.codec != null;
  }

  private int maxBytesToConsume(long pos) {
    return this.isCompressedInput()?2147483647:(int)Math.min(2147483647L, this.end - pos);
  }

  private long getFilePosition() throws IOException {
    long retVal;
    if(this.isCompressedInput() && null != this.filePosition) {
      retVal = this.filePosition.getPos();
    } else {
      retVal = this.pos;
    }

    return retVal;
  }

  public synchronized boolean next(Text key, Text value) throws IOException {
    while(this.getFilePosition() <= this.end) {
      key.set(this.filename);
      int newSize = this.in.readLine(value, this.maxLineLength, Math.max(this.maxBytesToConsume(this.pos), this.maxLineLength));
      if(newSize == 0) {
        return false;
      }

      this.pos += (long)newSize;
      if(newSize < this.maxLineLength) {
        return true;
      }

      LOG.info("Skipped line of size " + newSize + " at pos " + (this.pos - (long)newSize));
    }

    return false;
  }

  public synchronized float getProgress() throws IOException {
    return this.start == this.end?0.0F:Math.min(1.0F, (float)(this.getFilePosition() - this.start) / (float)(this.end - this.start));
  }

  public synchronized long getPos() throws IOException {
    return this.pos;
  }

  public synchronized void close() throws IOException {
    try {
      if(this.in != null) {
        this.in.close();
      }
    } finally {
      if(this.decompressor != null) {
        CodecPool.returnDecompressor(this.decompressor);
      }

    }

  }

  /** @deprecated */
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

    LineReader(InputStream in, byte[] recordDelimiter) {
      super(in, recordDelimiter);
    }

    LineReader(InputStream in, int bufferSize, byte[] recordDelimiter) {
      super(in, bufferSize, recordDelimiter);
    }

    public LineReader(InputStream in, Configuration conf, byte[] recordDelimiter) throws IOException {
      super(in, conf, recordDelimiter);
    }
  }
}

