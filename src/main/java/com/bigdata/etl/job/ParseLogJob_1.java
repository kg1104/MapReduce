package com.bigdata.etl.job;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.bigdata.mr.LogBeanWritable;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;


/**
 * 类描述：Mapper把日志文件并组装成bean类，Reducer接收bean类并把解析结果以json格式输出到hdfs
 * Mapper和Reducer之间的数据传输是LogBeanWritable类
 * */
public class ParseLogJob_1 extends Configured implements Tool {
    public static LogBeanWritable parseLog(String row) throws ParseException {
        String[] logPart = StringUtils.split(row, "\u1111");
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        long timeTag = dateFormat.parse(logPart[0]).getTime();
        String activeName = logPart[1];
        JSONObject bizData = JSON.parseObject(logPart[2]);

        LogBeanWritable logData = new LogBeanWritable();
        logData.setActiveName(activeName);
        logData.setTimeTag(timeTag);
        logData.setDeviceID(bizData.getString("device_id"));
        logData.setIp(bizData.getString("ip"));
        logData.setOrderID(bizData.getString("order_id"));
        logData.setProductID(bizData.getString("product_id"));
        logData.setReqUrl(bizData.getString("req_url"));
        logData.setSessionID(bizData.getString("session_id"));
        logData.setUserID(bizData.getString("user_id"));

        return logData;
    }

    public static class LogMapper extends Mapper<LongWritable, Text, LongWritable, LogBeanWritable> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            try {
                LogBeanWritable parsedLog = parseLog(value.toString());
                context.write(key, parsedLog);
            } catch (ParseException e) {
                e.printStackTrace();
            }
        }
    }

    public static class LogReducer extends Reducer<LongWritable, LogBeanWritable, NullWritable, Text> {
        public void reduce(LongWritable key, Iterable<LogBeanWritable> values, Context context) throws IOException, InterruptedException {
            for (LogBeanWritable log : values) {
                context.write(null, new Text(log.asJsonString()));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new ParseLogJob_1(), args);
        System.exit(res);
    }

    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        Job job = Job.getInstance(conf);
        job.setJarByClass(ParseLogJob_1.class);
        job.setJobName("parselog");
        job.setMapperClass(LogMapper.class);
        // 不需要使用Reduce时设置num为0
//        job.setNumReduceTasks(0);
        // 使用Reduce时需要设置Map的输出类型和Reduce的输出类型
        job.setReducerClass(LogReducer.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(LogBeanWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        Path outputPath = new Path(args[1]);
        FileOutputFormat.setOutputPath(job, outputPath);

        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }
        if (!job.waitForCompletion(true)) {
            throw new RuntimeException(job.getJobName() + " failed!");
        }
        return 0;
    }
}
