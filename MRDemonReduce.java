package org.bigdata.hpsk.HadoopTest.MR;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * first KEYIN, VALUEIN, KEYOUT, VALUEOUT
 * @author hpsk
 *
 */
public class MRDemonReduce extends Reducer<Text, IntWritable, Text, IntWritable> {
@Override
protected void reduce(Text arg0, Iterable<IntWritable> arg1,
		Reducer<Text, IntWritable, Text, IntWritable>.Context arg2) throws IOException, InterruptedException {
	// TODO Auto-generated method stub
	/**
	 * customer the function
	 */
}
}
