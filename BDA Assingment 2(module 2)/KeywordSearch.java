import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class KeywordSearch {

    public static class SearchMapper
        extends Mapper<Object, Text, Text, Text>{

        private Text result = new Text();
        private String keyword = "hadoop";

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString();

            if (line.toLowerCase().contains(keyword)) {
                result.set(line);
                context.write(new Text("Found"), result);
            }
        }
    }

    public static class IdentityReducer
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
        Job job = Job.getInstance(conf, "keyword search");

        job.setJarByClass(KeywordSearch.class);
        job.setMapperClass(SearchMapper.class);
        job.setReducerClass(IdentityReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
