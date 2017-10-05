package org.bigdata.hpsk.HadoopTest.MR;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * first : KEYIN, VALUEIN, KEYOUT, VALUEOUT
 * @author hpsk
 *
 */
public class MRDemonMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		/**
		 * scond : define customer function
		 */
	}
}
