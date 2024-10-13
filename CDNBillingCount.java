import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CDNBillingCount {

    // Mapper class
    // LongWritable for the byte offset of the current line, Text for the line
    // itself
    // Text for the word output, and DoubleWritable for request count and bytes
    // transferred
    public static class TokenizerMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

        private final static DoubleWritable one = new DoubleWritable(1);
        private DoubleWritable bytesTransferred = new DoubleWritable();

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();

            // Extracting the last number from the line
            int lastSpace = line.lastIndexOf(" ");
            String lastNumber = line.substring(lastSpace + 1);

            if (lastNumber.equals("-"))
                bytesTransferred.set(0);
            else
                bytesTransferred.set(Integer.parseInt(lastNumber));

            context.write(new Text("bytes"), bytesTransferred);
            context.write(new Text("requests"), one);
        }
    }

    public static class DoubleSumReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        private TreeMap<String, Double> countMap = new TreeMap<String, Double>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            countMap.put("bytes", 0.0);
            countMap.put("requests", 0.0);
        }

        public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {

            // Sum the number of bytes transferred and number of requests
            double sum = 0;

            for (DoubleWritable val : values) {
                sum += val.get();
            }

            // Store the sum of bytes transferred and number of requests
            countMap.put(key.toString(), countMap.get(key.toString()) + sum);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (Map.Entry<String, Double> entry : countMap.entrySet()) {
                context.write(new Text(entry.getKey()), new DoubleWritable(entry.getValue()));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(CDNBillingCount.class);
        job.setMapperClass(TokenizerMapper.class);

        // Combiner class
        job.setCombinerClass(DoubleSumReducer.class);

        job.setReducerClass(DoubleSumReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        // Set the number of reducers to 1 for cumulative output
        job.setNumReduceTasks(1);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
