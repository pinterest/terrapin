package com.pinterest.terrapin.hadoop.examples;

import com.pinterest.terrapin.hadoop.HadoopJobLoader;
import com.pinterest.terrapin.hadoop.TerrapinUploaderOptions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Example WordCount program writing the data directly to terrapin.
 */
public class WordCount extends Configured implements Tool {

  public static class TokenizerMapper
      extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context)
        throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) {
        word.set(itr.nextToken());
        context.write(word, one);
      }
    }
  }

  public static class IntSumReducer
      extends Reducer<Text, IntWritable, BytesWritable, BytesWritable> {

    public void reduce(Text key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      context.write(new BytesWritable(key.toString().getBytes(), key.toString().length()),
          new BytesWritable(Integer.toString(sum).getBytes()));
    }
  }

  public int run(String[] args) throws Exception {
    TerrapinUploaderOptions options = TerrapinUploaderOptions.initFromSystemProperties();

    // Create the job, setting the inputs and map output key and map output value classes.
    // Also, set reducer and mapper.
    Job job = Job.getInstance(super.getConf(), "word count");
    job.setJarByClass(WordCount.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setReducerClass(IntSumReducer.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));

    // Wrap around Hadoop Loader job to write the data to a terrapin fileset.
    return new HadoopJobLoader(options, job).waitForCompletion() ? 0 : 1;
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    int res = ToolRunner.run(conf, new WordCount(), args);
    System.exit(res);
  }
}