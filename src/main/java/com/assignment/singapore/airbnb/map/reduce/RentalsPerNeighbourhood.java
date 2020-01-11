package com.assignment.singapore.airbnb.map.reduce;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class RentalsPerNeighbourhood {
    public static class NeighbourhoodGroupRentalCountMapper extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text neighbourhoodGroupKey = new Text();

        private String[] statesArray = new String[] { "Central Region", "East Region", "North Region",
                "North-East Region", "West Region" };

        private HashSet<String> states = new HashSet<String>(Arrays.asList(statesArray));

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            if (value.toString().contains("id,name,host_id,host_name,")) {
                // Skip header line (first line) of CSV
                return;
            }
            String val = value.toString();
            String data[] = val.split(",", -1);
            String neighbourhoodGroup = "";
            for (String state : states) {
                for (int i = 0; i < data.length; i++) {
                    if (data[i].contains(state)) {
                        neighbourhoodGroup = data[i];
                    }
                }
            }
            // .get will return null if the key is not there
            if (neighbourhoodGroup.isEmpty()) {
                // skip this record
                return;
            }
            neighbourhoodGroupKey.set(neighbourhoodGroup.trim());
            context.write(neighbourhoodGroupKey, one);
        }
    }

    public static class NeighbourhoodGroupRentalCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }

            result.set(sum);
            context.write(key, result);

        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: Rental Count for each neighbourhood group <in> <out>");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "Rental Count for each neighbourhood group");
        job.setJarByClass(RentalsPerNeighbourhood.class);
        job.setMapperClass(NeighbourhoodGroupRentalCountMapper.class);
        job.setCombinerClass(NeighbourhoodGroupRentalCountReducer.class);
        job.setReducerClass(NeighbourhoodGroupRentalCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
