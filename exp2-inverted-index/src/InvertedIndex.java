import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class InvertedIndex {

    public static class InvertedIndexMapper
            extends Mapper<LongWritable, Text, Text, Text> {

        private Text word = new Text();
        private Text docId = new Text();
        private Set<String> stopWords = new HashSet<>();

        @Override
        protected void setup(Context context) throws IOException {
            URI[] cacheFiles = context.getCacheFiles();
            if (cacheFiles != null && cacheFiles.length > 0) {
                Configuration conf = context.getConfiguration();
                FileSystem fs = FileSystem.get(cacheFiles[0], conf);
                FSDataInputStream in = fs.open(new Path(cacheFiles[0]));
                BufferedReader reader = new BufferedReader(new InputStreamReader(in));
                String line;
                while ((line = reader.readLine()) != null) {
                    stopWords.add(line.trim().toLowerCase());
                }
                reader.close();
            }
        }

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            String fileName = fileSplit.getPath().getName();
            docId.set(fileName);

            StringTokenizer tokenizer = new StringTokenizer(
                    value.toString().toLowerCase(), " \t\n\r\f,.;:!?\"'()-[]{}<>/\\|@#$%^&*+=~`");
            while (tokenizer.hasMoreTokens()) {
                String token = tokenizer.nextToken();
                if (token.length() > 1 && !stopWords.contains(token)
                        && !token.matches("\\d+")) {
                    word.set(token);
                    context.write(word, docId);
                }
            }
        }
    }

    public static class InvertedIndexCombiner
            extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            TreeMap<String, Integer> docCount = new TreeMap<>();
            for (Text val : values) {
                String doc = val.toString();
                docCount.put(doc, docCount.getOrDefault(doc, 0) + 1);
            }

            StringBuilder sb = new StringBuilder();
            for (java.util.Map.Entry<String, Integer> entry : docCount.entrySet()) {
                if (sb.length() > 0) sb.append(";");
                sb.append(entry.getKey()).append(",").append(entry.getValue());
            }
            context.write(key, new Text(sb.toString()));
        }
    }

    public static class InvertedIndexReducer
            extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            TreeMap<String, Integer> docCount = new TreeMap<>();
            for (Text val : values) {
                String[] pairs = val.toString().split(";");
                for (String pair : pairs) {
                    if (pair.contains(",")) {
                        String[] parts = pair.split(",", 2);
                        String doc = parts[0];
                        int count = Integer.parseInt(parts[1]);
                        docCount.put(doc, docCount.getOrDefault(doc, 0) + count);
                    }
                }
            }

            int total = 0;
            StringBuilder sb = new StringBuilder();
            for (java.util.Map.Entry<String, Integer> entry : docCount.entrySet()) {
                if (sb.length() > 0) sb.append(";");
                sb.append("<").append(entry.getKey()).append(",")
                  .append(entry.getValue()).append(">");
                total += entry.getValue();
            }
            sb.append(";<total,").append(total).append(">");

            context.write(key, new Text(sb.toString()));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "inverted index");
        job.setJarByClass(InvertedIndex.class);
        job.setMapperClass(InvertedIndexMapper.class);
        job.setCombinerClass(InvertedIndexCombiner.class);
        job.setReducerClass(InvertedIndexReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.addCacheFile(new URI(args[2] + "#stopwords"));

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
