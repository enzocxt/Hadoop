package tripDist;

import java.io.IOException;
import java.util.Formatter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class TripLength {

  public static class TripLengthMapper
          extends Mapper<Object, Text, DoubleWritable, IntWritable>{
    private final static IntWritable one = new IntWritable(1);
    private DoubleWritable dist = new DoubleWritable(0.0);

    public  void map(Object key, Text value, Context context
                    ) throws  IOException, InterruptedException {
      String[] line = value.toString().split(" ");
      // <taxi-id> <start date> <start pos (lat)> <start pos (long)>
      //           <end date> <end pos (lat)> <end pos (long)>
      double t1 = Double.parseDouble(line[1]);
      double t2 = Double.parseDouble(line[4]);
      double startLat = Double.parseDouble(line[2]);
      double startLong = Double.parseDouble(line[3]);
      double stopLat = Double.parseDouble(line[5]);
      double stopLong = Double.parseDouble(line[6]);

      // Compute the distance
      double distance, R = 6371.009;  // kilometer
      distance = Distance.computeDistance(startLat, startLong, stopLat, stopLong);

      String tmp = new Formatter().format("%.1f", distance).toString();
      double esDist = Double.parseDouble(tmp);

      if ((t2-t1) < 5.0) {
        return;
      } else if (distance > 2000.0) {
        return;
      } else if (distance / (t2-t1) < 0.0028 || distance / (t2-t1) > 0.056) {
        return;
      }else {
        dist.set(esDist);
        context.write(dist, one);
      }
    }
  }

  public static class IntSumReducer
          extends Reducer<DoubleWritable, IntWritable, DoubleWritable, IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(DoubleWritable key, Iterable<IntWritable> values,
                       Context context
                       ) throws  IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val: values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Trip Length");

    job.setJarByClass(TripLength.class);
    job.setMapperClass(TripLengthMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(DoubleWritable.class);
    job.setOutputValueClass(IntWritable.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
