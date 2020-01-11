/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.assignment.singapore.airbnb.map.reduce;

import java.io.IOException;

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

/**
 *
 * @author Acer
 */
public class WholeYearRental {

    public static class RentalCountMapper extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            if (value.toString().contains("id,name,host_id,host_name,")) {
                // Skip header line (first line) of CSV
                return;
            }
            String data[] = value.toString().split(",");
            String availability = "";
            if ("365".equalsIgnoreCase(data[data.length - 1])) {
                availability = data[data.length - 1];
            }

            // .get will return null if the key is not there
            if (availability.isEmpty()) {
                // skip this record
                return;
            }
            word.set(availability.trim());
            context.write(word, one);
        }
    }

    public static class RentalCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
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
            System.err.println("Usage: 365 day rental count <in> <out>");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "365 day rental count");
        job.setJarByClass(WholeYearRental.class);
        job.setMapperClass(RentalCountMapper.class);
        job.setCombinerClass(RentalCountReducer.class);
        job.setReducerClass(RentalCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
