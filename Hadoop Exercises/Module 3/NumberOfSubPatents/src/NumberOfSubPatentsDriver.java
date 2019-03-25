import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class NumberOfSubPatentsDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration configuration = new Configuration();
        Job job = new Job(configuration);

        job.setJarByClass(NumberOfSubPatentsDriver.class);
        job.setMapperClass(NumberOfSubPatentsMapper.class);
        job.setReducerClass(NumberOfSubPatentsReducer.class);
        job.setNumReduceTasks(4);

        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(LongWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        FileSystem fileSystem = FileSystem.get(configuration);
        fileSystem.delete(new Path(args[2]));

        System.exit(job.waitForCompletion(true) ? 0:1);

    }

}
