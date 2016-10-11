import java.io.BufferedReader;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.StringUtils;

public class SimilarUsers {

  public static class IntStringPair
          implements WritableComparable<IntStringPair> {
    private int first = 0;
    private String second = "";

    /**
     * Set the left and right values.
     */
    public void set(int left, String right) {
      first = left;
      second = right;
    }
    public int getFirst() {
      return first;
    }
    public String getSecond() {
      return second;
    }
    @Override
    public void readFields(DataInput in) throws IOException {
      first = in.readInt() + Integer.MIN_VALUE;
      second = in.readLine();
    }
    @Override
    public void write(DataOutput out) throws IOException {
      out.writeInt(first - Integer.MIN_VALUE);
      out.writeChars(second);
    }
    @Override
    public int hashCode() {
      return first * 157 + second.hashCode();
    }
    @Override
    public boolean equals(Object right) {
      if (right instanceof IntStringPair) {
        IntStringPair r = (IntStringPair) right;
        return r.first == first && r.second == second;
      } else {
        return false;
      }
    }
    /** A Comparator that compares serialized IntStringPair. */
    public static class Comparator extends WritableComparator {
      public Comparator() {
        super(IntStringPair.class);
      }

      public int compare(byte[] b1, int s1, int l1,
                         byte[] b2, int s2, int l2) {
        return compareBytes(b1, s1, l1, b2, s2, l2);
      }
    }

    static {                                        // register this comparator
      WritableComparator.define(IntStringPair.class, new Comparator());
    }

    @Override
    public int compareTo(IntStringPair o) {
      if (first != o.first) {
        return first < o.first ? -1 : 1;
      } else if (second != o.second) {
        return second.compareTo(o.second) < 0 ? -1 : 1;
      } else {
        return 0;
      }
    }

    @Override
    public String toString() {
      return first + " " + second;
    }
  }

  public static class UserLikeMapper
          extends Mapper<Object, Text, IntStringPair, IntWritable>{

    private final IntStringPair outKey = new IntStringPair();
    private final IntWritable outValue = new IntWritable();

    @Override
    public void map(Object key, Text value, Context context
    ) throws IOException, InterruptedException {
      String line = value.toString();
/*      String line = (caseSensitive) ?
          value.toString() : value.toString().toLowerCase();
      for (String pattern : patternsToSkip) {
        line = line.replaceAll(pattern, "");
      }*/
      String[] ratingLine = line.split(",");
      if (ratingLine.length >= 4) {
        try {
          int userId = new Integer(ratingLine[0]);
          int movieId = new Integer(ratingLine[1]);
          double rating = new Double(ratingLine[2]);
          long timestamp = new Long(ratingLine[3]);

          if (0.5 <= rating && rating <= 2.5) {
            outKey.set(userId, "unlike");
          } else if (2.5 < rating && rating <= 5.0) {
            outKey.set(userId, "like");
          }
          outValue.set(movieId);
          context.write(outKey, outValue);
        } catch (Exception e) {
        }
      }
    }
  }

  public static class MovieArrayWritable extends ArrayWritable {
    MovieArrayWritable() {
      super(IntWritable.class);
    }

    @Override
    public String toString() {
      return Arrays.toString(get());
    }
  }

  public static class MovieReducer
          extends Reducer<IntStringPair,IntWritable,IntStringPair,MovieArrayWritable> {
    private MovieArrayWritable result = new MovieArrayWritable();
    private final static IntWritable ONE = new IntWritable(1);

    public void reduce(IntStringPair key, Iterable<IntWritable> values,
                       Context context
    ) throws IOException, InterruptedException {
      List<IntWritable> iwList = new ArrayList<IntWritable>();
      for (IntWritable val : values) {
        iwList.add(new IntWritable(val.get()));
      }
      IntWritable[] iValues = iwList.toArray(new IntWritable[iwList.size()]);
      Arrays.sort(iValues);
      result.set(iValues);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    GenericOptionsParser optionParser = new GenericOptionsParser(conf, args);
    String[] remainingArgs = optionParser.getRemainingArgs();
    /*if (!(remainingArgs.length != 2 || remainingArgs.length != 4)) {
      System.err.println("Usage: wordcount <in> <out> [-skip skipPatternFile]");
      System.exit(2);
    }
    Job job = Job.getInstance(conf, "word count");*/
    Job job = Job.getInstance(conf, "top-100 pairs of similar users with similarity");
    job.setJarByClass(SimilarUsers.class);
    job.setMapperClass(UserLikeMapper.class);
    // job.setCombinerClass(IntSumReducer.class);
    // job.setReducerClass(IntSumReducer.class);
    job.setReducerClass(MovieReducer.class);
    // job.setReducerClass(Reducer.class);
    job.setOutputKeyClass(IntStringPair.class);
    // job.setOutputValueClass(SortedMapWritable.class);
    // job.setOutputValueClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    List<String> otherArgs = new ArrayList<String>();
    for (int i=0; i < remainingArgs.length; ++i) {
      // if ("-skip".equals(remainingArgs[i])) {
      //   job.addCacheFile(new Path(remainingArgs[++i]).toUri());
      //   job.getConfiguration().setBoolean("wordcount.skip.patterns", true);
      // } else {
      otherArgs.add(remainingArgs[i]);
      // }
    }
    FileInputFormat.addInputPath(job, new Path(otherArgs.get(0)));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs.get(1)));

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
