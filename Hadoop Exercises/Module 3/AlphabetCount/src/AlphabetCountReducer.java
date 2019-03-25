import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class AlphabetCountReducer extends Reducer<LongWritable, LongWritable, LongWritable, LongWritable> {

    LongWritable value = new LongWritable();
    public void reduce(LongWritable key, Iterable<LongWritable> values, Context context) throws IOException,
            InterruptedException {

        long sum = 0;

        for (LongWritable val: values){
            sum = sum + val.get();
        }

        value.set(sum);
        context.write(key, value);

    }
}