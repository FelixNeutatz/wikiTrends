package io.sanfran.wikiTrends.extraction.hadoop;

import java.io.IOException;
import org.apache.flink.hadoop.shaded.com.google.common.base.Charsets;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Stable;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.SplittableCompressionCodec;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

public class FileNameTextInputFormat extends FileInputFormat<Text, Text> implements JobConfigurable {
  private CompressionCodecFactory compressionCodecs = null;

  public FileNameTextInputFormat() {
  }

  public void configure(JobConf conf) {
    this.compressionCodecs = new CompressionCodecFactory(conf);
  }

  protected boolean isSplitable(FileSystem fs, Path file) {
    CompressionCodec codec = this.compressionCodecs.getCodec(file);
    return null == codec?true:codec instanceof SplittableCompressionCodec;
  }

  public RecordReader<Text, Text> getRecordReader(InputSplit genericSplit, JobConf job, Reporter reporter) throws IOException {
    reporter.setStatus(genericSplit.toString());
    String delimiter = job.get("textinputformat.record.delimiter");
    byte[] recordDelimiterBytes = null;
    if(null != delimiter) {
      recordDelimiterBytes = delimiter.getBytes(Charsets.UTF_8);
    }

    return new FileNameLineRecordReader(job, (FileSplit)genericSplit, recordDelimiterBytes);
  }
}
