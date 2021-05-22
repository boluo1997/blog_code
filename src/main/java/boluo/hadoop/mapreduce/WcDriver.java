package boluo.hadoop.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 对任务进行一些相关的设置
 */
public class WcDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        // 1.获取一个Job实例
        Job job = Job.getInstance(new Configuration());

        // 2.设置类路径
        job.setJarByClass(WcDriver.class);

        // 3.设置Mapper和Reduce
        job.setMapperClass(WcMapper.class);
        job.setReducerClass(WcReducer.class);

        // 4.设置Mapper和Reducer输出的类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 5.设置输入输出数据源
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 6.提交Job
        boolean br = job.waitForCompletion(true);
        System.out.println(br ? 0 : -1);

    }
}





