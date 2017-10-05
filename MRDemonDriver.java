package org.bigdata.hpsk.HadoopTest.MR;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MRDemonDriver extends Configured implements Tool{

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		//create the configuration instance
//		
		//exec the run 
		try {
			int status = ToolRunner.run(new MRDemonDriver(), args);
			System.exit(status);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		//new the mapreduce job
		Job job = Job.getInstance(conf, "job-name");
		job.setJarByClass(null);
		//input
		Path inputPaths = new Path(args[0]);
		FileInputFormat.setInputPaths(job, inputPaths);
		//map
		job.setMapperClass(null);
		job.setMapOutputKeyClass(null);
		job.setMapOutputValueClass(null);
		//shuffle
		job.setSortComparatorClass(null);
		job.setPartitionerClass(null);
		job.setGroupingComparatorClass(null);
		//reduce
		job.setReducerClass(null);
		job.setOutputKeyClass(null);
		job.setOutputValueClass(null);
		job.setNumReduceTasks(1);
		//output
		Path outputDir = new Path(args[1]);
		FileOutputFormat.setOutputPath(job, outputDir);
		//submit
		boolean isSuccess = job.waitForCompletion(true);
		return isSuccess ? 0:1;
	}

}
