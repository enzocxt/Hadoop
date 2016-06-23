package tripRevenue;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.text.DateFormat;
import java.text.ParseException;
import java.util.Locale;
import java.util.TimeZone;


public class RideRevenue {

  public static class TripMapper
          extends Mapper<Object, Text, Entry, Text>{
    private Entry entry = new Entry();
    private Text pos = new Text();

    public  void map(Object key, Text value, Context context
                    ) throws  IOException, InterruptedException {
      String[] values = value.toString().split(",");

      // check if this segment is valuable
      // <taxi-id> <start date> <start pos (lat)> <start pos (long)> <start state>
      //           <end date> <end pos (lat)> <end pos (long)> <end state>
      if (values.length != 9) { return; }
      String t1, t2;
      t1 = values[1].replace("'", "");
      t2 = values[5].replace("'", "");
      TimeZone.setDefault(TimeZone.getTimeZone("America/San Francisco"));
      DateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.ENGLISH);
      Date d1 = new Date(), d2 = new Date();
      try { d1 = format.parse(t1); }
      catch (ParseException pe) { return; }
      try { d2 = format.parse(t2); }
      catch (ParseException pe) { return; }
      int[] indexOfCoors = {2, 3, 6, 7};
      for (int i : indexOfCoors) {
        try { double c = Double.parseDouble(values[i]); }
        catch (Exception e) { return; }
      }
      String state1 = values[4].replace("'", ""), state2 = values[8].replace("'", "");
      if (!"EM".contains(state1) || !"EM".contains(state2)) { return; }
      if (state1.compareTo("E") == 0 && state2.compareTo("E") == 0) { return; }
      // end of checking

      // the segment is valuable
      int taxiId = Integer.parseInt(values[0]);
      entry.setTaxiId(taxiId);
      entry.setStartTime(values[1].replace("'", ""));

      String info = value.toString().substring(values[0].length()+1);
      pos.set(info);

      context.write(entry, pos);
    }
  }

  public static class TripReducer
          extends Reducer<Entry, Text, IntWritable, Text> {
    private final static IntWritable taxiId = new IntWritable();
    private Text resultTrips = new Text();
    public void reduce(Entry key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
      taxiId.set(key.getTaxiId());

      TaxiTrips taxi = new TaxiTrips();
      taxi.setTaxiId(key.getTaxiId());

      ArrayList<String> sValues = new ArrayList<String>();
      for (Text value : values) { sValues.add(value.toString()); }

      Trip trip = new Trip();
      boolean valuable = true;
      for (int i = 0; i < sValues.size(); i++) {
        String[] info = sValues.get(i).toString().split(",");
        if (info.length != 8) { continue; }

        // start time and stop time
        String t1, t2;
        t1 = info[0].replace("'", "");
        t2 = info[4].replace("'", "");
        // start state and stop state
        String state1, state2;
        state1 = info[3].replace("'", "");
        state2 = info[7].replace("'", "");
        // start coordinate and stop coordinate
        String startLat = info[1], startLong = info[2];
        String stopLat = info[5], stopLong = info[6];

        // check taxi speed of this segment
        double dStartLat = Double.parseDouble(startLat), dStartLong = Double.parseDouble(startLong);
        double dStopLat = Double.parseDouble(stopLat), dStopLong = Double.parseDouble(stopLong);
        double distance = Distance.computeDistance(dStartLat, dStartLong, dStopLat, dStopLong);
        double speed = 0.0;
        TimeZone.setDefault(TimeZone.getTimeZone("America/San Francisco"));
        DateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.ENGLISH);
        Date d1 = new Date(), d2 = new Date();
        try { d1 = format.parse(t1); }
        catch (ParseException pe) { valuable = false; }
        try { d2 = format.parse(t2); }
        catch (ParseException pe) { valuable = false; }
        if (t1 == t2) { valuable = false; }
        else { speed = distance / ((double) Math.abs(d1.getTime() - d2.getTime()) / 1000.0); }
        if (speed > 0.056) { valuable = false; }
        // end of checking

        if (i == 0) {
          if (state1.compareTo(state2) == 0) {
            trip.addCoor(startLat, startLong);
            trip.addCoor(stopLat, stopLong);
            trip.setStartTime(t1);
          } else {
            trip.addCoor(stopLat, stopLong);
            trip.setStartTime(t2);
          }
        } else {
          if (state1.compareTo(state2) == 0) {
            // if the two states are the same
            // and this segment is not the first one:
            // the startLat and startLong are the same as
            // the stop coordinate of last segment
            trip.addCoor(stopLat, stopLong);
          } else if (state1.compareTo("M") == 0 && state2.compareTo("E") == 0) {
            // add this trip to taxi Trips
            if (valuable) { taxi.addTrip(trip); }
          } else if (state1.compareTo("E") == 0 && state2.compareTo("M") == 0) {
            trip = new Trip();
            trip.addCoor(stopLat, stopLong);
            trip.setStartTime(t2);
            valuable = true;
          }
        }

        // the last value and the two states are the same
        if (i == sValues.size()-1 && state1 == state2) {
          taxi.addTrip(trip);
        }
      }
      resultTrips.set(taxi.toString());
      context.write(taxiId, resultTrips);
    }
  }

  public static class RevenueMapper
          extends Mapper<Object, Text, Text, DoubleWritable>{
    private Text date = new Text();
    private DoubleWritable rev = new DoubleWritable(0.0);

    public  void map(Object key, Text value, Context context
    ) throws  IOException, InterruptedException {
      double revenue = 3.50;

      String line;
      if (value.toString().contains("\t")) {
        if (value.toString().split("\t").length > 1) {
          line = value.toString().split("\t")[1];
        } else { return; }
      } else { line = value.toString(); }

      if (!line.contains(":") || line.split(":").length != 3) { return; }
      String[] values = line.split(":");
      int taxiId = Integer.parseInt(values[0]);
      String startDate = values[1];
      date.set(startDate);
      String[] coordinates = values[2].split(" ");

      // Airport latitude and longtitude
      double circleLat = 37.62131, circleLong = -122.37896;
      boolean passAirport = false;
      for (int i = 0; i < coordinates.length-1; i++) {
        String start = coordinates[i], stop = coordinates[i+1];
        double startLat, startLong, stopLat, stopLong;
        startLat = Double.parseDouble(start.split(",")[0]);
        startLong = Double.parseDouble(start.split(",")[1]);
        stopLat = Double.parseDouble(stop.split(",")[0]);
        stopLong = Double.parseDouble(stop.split(",")[1]);

        if (!passAirport) {
          passAirport = Distance.passCircle(circleLat, circleLong, startLat, startLong, stopLat, stopLong);
        }
        revenue += 1.71 * Distance.computeDistance(startLat, startLong, stopLat, stopLong);
      }

      if (!passAirport) {
        return;
      } else {
        rev.set(revenue);
        context.write(date, rev);
      }
    }
  }

  public static class RevenueReducer
          extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
    private DoubleWritable result = new DoubleWritable(0.0);
    public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
            throws IOException, InterruptedException {
      double sum = 0.0;
      for (DoubleWritable value : values) {
        sum += value.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }


  public static void main(String[] args) throws Exception {

    JobConf conf = new JobConf(RideRevenue.class);

    Job jobTrips = Job.getInstance(conf, "ReconstructTrips");
    //FileInputFormat.setMaxInputSplitSize(jobTrips, 33554432l);
    jobTrips.setNumReduceTasks(80);
    jobTrips.setJarByClass(RideRevenue.class);
    jobTrips.setMapperClass(TripMapper.class);
    jobTrips.setReducerClass(TripReducer.class);
    jobTrips.setPartitionerClass(EntryPartitioner.class);
    jobTrips.setGroupingComparatorClass(EntryGroupingComparator.class);
    jobTrips.setOutputKeyClass(Entry.class);
    jobTrips.setOutputValueClass(Text.class);

    FileInputFormat.addInputPath(jobTrips, new Path(args[0]));
    FileOutputFormat.setOutputPath(jobTrips, new Path(args[1]));

    Job jobRevenue = Job.getInstance(conf, "CalculateRevenue");
    jobRevenue.setJarByClass(RideRevenue.class);
    jobRevenue.setMapperClass(RevenueMapper.class);
    jobRevenue.setReducerClass(RevenueReducer.class);
    jobRevenue.setOutputKeyClass(Text.class);
    jobRevenue.setOutputValueClass(DoubleWritable.class);

    FileInputFormat.addInputPath(jobRevenue, new Path(args[1]));
    FileOutputFormat.setOutputPath(jobRevenue, new Path(args[2]));

    if (jobTrips.waitForCompletion(true)) {
      System.exit(jobRevenue.waitForCompletion(true) ? 0 : 1);
    }

  }
}
