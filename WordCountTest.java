package org.bigdata.hpsk.HadoopTest.MR;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
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

public class WordCountTest {
	
	/**
	 * mapper class
	 * 	KEYIN type, VALUEIN type , KEYOUT type, VALUEOUT type
	 * @author hpsk
	 *
	 */
	public static class wcMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
		
		private Text outputKey = new Text();
		private IntWritable outputVaule = new IntWritable(1);
		
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String line = value.toString();
			String[] words = line.split(" ");
			for(String word : words ){
				this.outputKey.set(word);
				System.out.println("map out "+ word+"\t"+outputVaule.get());
				context.write(outputKey, outputVaule);
			}
		}
	}
	
	/**
	 * wordcount reduce class 
	 * 	KEYIN, VALUEIN, KEYOUT, VALUEOUT
	 * @author hpsk
	 *
	 */
	public static class wcReducer extends Reducer< Text, IntWritable, Text, IntWritable>{
		
		private IntWritable outputValue = new IntWritable();
		
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,Context context) 
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			int sum = 0 ;
			for(IntWritable value : values){
				sum += value.get();
			}
			
			this.outputValue.set(sum);
			System.out.println("reduce out "+ key.toString()+"\t"+this.outputValue.get());
			context.write(key, outputValue);
		}
	}
	
	

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		//create the configuration instance
		Configuration conf = new Configuration();
		/**
		 * create the job instance
		 */
		Job job = Job.getInstance(conf, "wc-customer");
		job.setJarByClass(WordCountTest.class);
		/**
		 * input
		 */
		Path input = new Path(args[0]);
		FileInputFormat.setInputPaths(job, input);
		/**
		 * map
		 */
		job.setMapperClass(wcMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		/**
		 * shuffle
		 */
		
		/**
		 * reduce
		 */
		job.setReducerClass(wcReducer.class);
		job.setOutputKeyClass(Text.class);
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
		System.exit(isSuccess ? 0 : 1 );
	}

}
