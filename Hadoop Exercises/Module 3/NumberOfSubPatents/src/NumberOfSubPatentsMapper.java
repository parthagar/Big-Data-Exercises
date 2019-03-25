import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class NumberOfSubPatentsMapper extends Mapper<LongWritable, Text, LongWritable, LongWritable> {
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String[] strArr = value.toString().split("\\W+");

            LongWritable ContextKey = new LongWritable(Long.parseLong(strArr[0]));
            LongWritable ContextValue = new LongWritable(1);

            context.write(ContextKey, ContextValue);

    }
}