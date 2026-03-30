import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SortByName {

    public static class SortMapper
        extends Mapper<Object, Text, Text, Text>{

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            String[] parts = value.toString().split(" ");

            if (parts.length >= 3) {
                // KEY = name (for sorting)
                // VALUE = full record
                context.write(new Text(parts[1]), value);
            }
        }
    }

    public static class SortReducer
        extends Reducer<Text, Text, Text, Text>{

        public void reduce(Text key, Iterable<Text> values,
                           Context context)
                throws IOException, InterruptedException {

            for (Text val : values) {
                context.write(key, val);
            }
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "sort by name");

        job.setJarByClass(SortByName.class);
        job.setMapperClass(SortMapper.class);
        job.setReducerClass(SortReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
