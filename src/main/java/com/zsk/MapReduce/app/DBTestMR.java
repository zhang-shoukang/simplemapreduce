package com.zsk.MapReduce.app;

import com.zsk.MapReduce.domain.StudentinfoRecord;
import com.zsk.MapReduce.mapreduce.MyDBTestMapper;
import com.zsk.MapReduce.mapreduce.MyDBTestReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.util.Date;

public class DBTestMR extends Configured implements Tool {
    public static void main(String[] args) throws Exception{
        int res = ToolRunner.run(new Configuration(), new DBTestMR(), args);
        System.err.println(res);
    }
    @Override
    public int run(String[] args) throws Exception {
        Configuration configuration = this.getConf();
        Job job = Job.getInstance(configuration, this.getClass().getSimpleName() + new Date());
        //DistributedCache.addFileToClassPath(new Path("hdfs://hadoop000:8020/tmp/jars/mysql-connector-java-5.1.47.jar"),configuration);
        job.addFileToClassPath(new Path("/tmp/jars/mysql-connector-java-5.1.47.jar"));
        job.setJarByClass(this.getClass());
        job.setMapperClass(MyDBTestMapper.class);
        job.setReducerClass(MyDBTestReducer.class);
        job.setOutputKeyClass(StudentinfoRecord.class);
        job.setOutputValueClass(NullWritable.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setInputFormatClass(org.apache.hadoop.mapreduce.lib.db.DBInputFormat.class);
        job.setOutputFormatClass(org.apache.hadoop.mapreduce.lib.db.DBOutputFormat.class);
        org.apache.hadoop.mapreduce.lib.db.DBConfiguration.configureDB(configuration, "com.mysql.jdbc.Driver", "jdbc:mysql://hadoop000:3306/testmr",
                "root", "root");
        org.apache.hadoop.mapreduce.lib.db.DBInputFormat.setInput(job, StudentinfoRecord.class, "select id,name from t","select count(1) from t");
        org.apache.hadoop.mapreduce.lib.db.DBOutputFormat.setOutput(job,"t2",StudentinfoRecord.FIELDS);
        return job.waitForCompletion(true)?0:1;
    }
}
