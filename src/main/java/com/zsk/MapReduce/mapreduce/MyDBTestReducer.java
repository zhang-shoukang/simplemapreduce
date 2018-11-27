package com.zsk.MapReduce.mapreduce;

import com.zsk.MapReduce.domain.StudentinfoRecord;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MyDBTestReducer extends Reducer<LongWritable ,Text, StudentinfoRecord, NullWritable> {
    private LongWritable outputKey;
    private Text outputValue;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            this.outputKey = new LongWritable(0);
            this.outputValue = new Text();
    }

    @Override
    protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        super.reduce(key, values, context);
        for (Text value : values) {
            ;
            context.write(new StudentinfoRecord((int)key.get(),value.toString()),NullWritable.get());
        }
    }
}
