package com.zsk.MapReduce.mapreduce;

import com.zsk.MapReduce.domain.StudentinfoRecord;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MyDBTestMapper extends Mapper<LongWritable, StudentinfoRecord,LongWritable, Text> {
    private LongWritable outputKey;
    private Text outputValue;
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        this.outputKey= new LongWritable(0);
        this.outputValue = new Text();
    }
    @Override
    protected void map(LongWritable key, StudentinfoRecord value, Context context) throws IOException, InterruptedException {
        super.map(key, value, context);
        outputKey.set(value.getId());
        outputValue.set(value.getName());
        context.write(outputKey,outputValue);
    }
}
