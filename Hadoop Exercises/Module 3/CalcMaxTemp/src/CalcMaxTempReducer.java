import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class CalcMaxTempReducer extends Reducer<LongWritable, LongWritable, LongWritable, LongWritable> {

    LongWritable value = new LongWritable();
    public void reduce(LongWritable key, Iterable<LongWritable> values, Context context) throws IOException,
            InterruptedException {

        long max = 0;

        for (LongWritable val: values){
            if(val.get() > max)
                max = val.get();
        }

        value.set(max);
        context.write(key, value);

    }
}