import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FindHotAndColdDaysMapper extends Mapper<LongWritable, Text, Text, Text> {
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String TextToString = value.toString();

        if (TextToString.length() != 0) {
            String[] strArr= TextToString.split("\\s+");

            String full_date = strArr[1];
            double max_temp = Double.parseDouble(strArr[5]);
            double min_temp = Double.parseDouble(strArr[6]);

            String date = full_date.substring(6, 8);
            String month = full_date.substring(4, 6);
            String year = full_date.substring(0, 4);

            Text ContextKey = new Text(date + "-" + month + "-" + year);

            if(max_temp > 40 && max_temp != -9999)
                context.write(ContextKey, new Text("Hot Day"));

            if(min_temp < 10 && min_temp != -9999)
                context.write(ContextKey, new Text("Cold Day"));
        }

    }
}