package com.zsk.MapReduce;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Iterator;

/**
 * Create by zsk on 2018/9/12
 **/

/**
 * 二次排序demo
 */
public class TestSecondarySort extends Configured implements Tool {

    @Override
    public void setConf(Configuration conf) {
        super.setConf(conf);

    }
    public static class MyMapper extends Mapper<LongWritable,Text,Text,LongWritable>{
        private   Text okey;
        private   LongWritable ovalue;
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            okey = new Text();
            ovalue = new LongWritable();
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String resLine = "";
            String[] split = StringUtils.split(value.toString(), ',');
            if (split.length!=2)
                return;
            String keys = split[0];
            String values = split[1];
            okey.set(keys+'&'+values);
            ovalue.set(Integer.parseInt(values));
            context.write(okey,ovalue);
        }
    }
    public static class MyReducer extends Reducer<Text,LongWritable,Text,Text> {
        private   Text okey;
        private   Text ovalue;
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            okey = new Text();
            ovalue = new Text();
        }

        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            okey.set(key.toString());
            Iterator<LongWritable> iterator = values.iterator();
            StringBuilder stringBuilder = new StringBuilder();
            while (iterator.hasNext()){

                LongWritable next = iterator.next();
                stringBuilder.append(next);
            }
            ovalue.set(stringBuilder.toString());
            context.write(okey,ovalue);

        }
    }
    @Override
    public int run(String[] args) throws Exception{
        String inPath = args[0];
        String outPath = args[1];
        Configuration conf = this.getConf();
        Job job = Job.getInstance(conf,"TestMRSecondSort");
        job.setInputFormatClass(FileInputFormat.class);
        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        job.setOutputFormatClass(FileOutputFormat.class);

        job.setPartitionerClass(MyPartitionr.class);
        job.setGroupingComparatorClass(MyGroupingComparator.class);

        FileInputFormat.addInputPath(job,new Path(inPath));
        FileOutputFormat.setOutputPath(job,new Path(outPath));
        return job.waitForCompletion(true)?0:-1;
    }

    public static void main(String[] args) throws Exception{
        int status = ToolRunner.run(new Configuration(), new TestSecondarySort(), args);
        System.exit(status);
    }
    public static class MyPartitionr extends org.apache.hadoop.mapreduce.Partitioner<Text,Text>{
        @Override
        public int getPartition(Text key, Text value, int numPartitions) {
            return key.toString().hashCode()%numPartitions;
        }
    }
    public static class MyGroupingComparator extends WritableComparator{
        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            String first = StringUtils.split(a.toString(), '&')[0];
            String second = StringUtils.split(b.toString(), '&')[0];
            return first.compareTo(second);
        }
    }
}
