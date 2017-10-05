package org.bigdata.hpsk.HadoopTest.MR;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WebLogPVMR extends Configured implements Tool{
	
	
	public static class pvMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable>{
		
		private IntWritable outputKey = new IntWritable();
		private IntWritable outputValue = new IntWritable(1);
		
		@Override
		protected void map(LongWritable key, Text value,Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String line = value.toString();
			String[] words = line.split("\t");
			//filter
			if(36 > words.length){
				context.getCounter("web-log", "length < 36").increment(1L);
				return;
			}
			String province = words[23];
			String url = words[1];
			if(StringUtils.isBlank(province) || StringUtils.isBlank(url)){
				context.getCounter("web-log", "isblank").increment(1L);
				return;
			}
			int provinceID = Integer.MAX_VALUE;
			try {
				provinceID = Integer.valueOf(province);
			} catch (Exception e) {
				// TODO: handle exception
				return;
			}
			if (provinceID == Integer.MAX_VALUE){
				return;
			}
			this.outputKey.set(provinceID);
			context.write(outputKey, outputValue);
		}
	}
	
	public static class pvReducer extends  Reducer<IntWritable, IntWritable, IntWritable, IntWritable>{
		
		private IntWritable outputValue = new IntWritable();
		
		@Override
		protected void reduce(IntWritable key, Iterable<IntWritable> values,Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			int sum = 0 ;
			for(IntWritable value : values){
				sum += value.get();
			}
			
			this.outputValue.set(sum);
			context.write(key, outputValue);
		}
	}
	

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		try {
			int status = ToolRunner.run(new WebLogPVMR(), args);
			System.exit(status);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		//create the configuration instance
		Configuration conf = new Configuration();
		/**
		 * create the job instance
		 */
		Job job = Job.getInstance(conf, "web-pv");
		job.setJarByClass(WebLogPVMR.class);
		/**
		 * input
		 */
		Path input = new Path(args[0]);
		FileInputFormat.setInputPaths(job, input);
		/**
		 * map
		 */
		job.setMapperClass(pvMapper.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(IntWritable.class);
		/**
		 * shuffle
		 */
		
		/**
		 * reduce
		 */
		job.setReducerClass(pvReducer.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);
		/**
		 * output
		 */
		Path output = new Path(args[1]);
		FileSystem file = FileSystem.get(conf);
		if(file.exists(output)){
			file.delete(output, true);
		}
		FileOutputFormat.setOutputPath(job, output);
		
		
		//submit
		boolean isSuccess = job.waitForCompletion(true);
		return isSuccess ? 0 : 1 ;
	}

}
