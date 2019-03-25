import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WordCountMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String TextToString = value.toString();
        String[] strArr= TextToString.split("\\W+");

        for(String str: strArr){
            Text ContextKey = new Text(str);
            LongWritable ContextValue = new LongWritable(1);
            context.write(ContextKey, ContextValue);
        }

    }
}