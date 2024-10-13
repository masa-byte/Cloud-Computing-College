import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount3or5 {

    // Mapper class
    // LongWritable for input key (byte offset), Text for input value (file line)
    // IntWritable for output key (word length), IntWritable for output value (count
    // = 1)
    public static class TokenizerMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private IntWritable wordLength = new IntWritable();

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line);
            while (tokenizer.hasMoreTokens()) {
                wordLength.set(tokenizer.nextToken().length());
                // Output word length with count 1
                context.write(wordLength, one);
            }
        }
    }

    // Reducer class
    // IntWritable for input key (word length), IntWritable for input value (count =
    // 1)
    // IntWritable for output key (word length), IntWritable for output value
    // (count)
    public static class IntSumReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
        // TreeMap to store the word lengths and their counts
        private TreeMap<Integer, Integer> countMap = new TreeMap<Integer, Integer>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            // Initialize the countMap with 0 counts for word lengths 3 and 5
            // More word lengths can be added if needed
            countMap.put(3, 0);
            countMap.put(5, 0);
        }

        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

            int newKey = key.get();

            // We only want to count words with 3 or 5 characters
            if (newKey == 3 || newKey == 5) {
                // Calculate the sum of the counts
                int sum = 0;
                for (IntWritable val : values) {
                    sum += val.get();
                }
                countMap.put(newKey, countMap.get(newKey) + sum);
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            List<Map.Entry<Integer, Integer>> list = new ArrayList<Map.Entry<Integer, Integer>>(countMap.entrySet());

            // Output the word lengths and their counts
            for (int i = 0; i < list.size(); i++) {
                Map.Entry<Integer, Integer> entry = list.get(i);
                context.write(new IntWritable(entry.getKey()), new IntWritable(entry.getValue()));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(WordCount3or5.class);
        job.setMapperClass(TokenizerMapper.class);
        // Combiner class
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        // Ouput key and value both are of type IntWritable
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);

        // Set the number of reducers to 1 for cumulative output
        job.setNumReduceTasks(1);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
