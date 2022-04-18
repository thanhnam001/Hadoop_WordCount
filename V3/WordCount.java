import java.io.IOException;
import java.util.StringTokenizer;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.net.URI;
import java.util.HashSet;
import java.util.Set;
import java.io.InputStreamReader;
import java.io.FileNotFoundException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WordCount {
    public static class Map extends Mapper<Object, Text, Text, IntWritable>{
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        private String path;
        private Set<String> stopwords = new HashSet<String>();

        @Override
        protected void setup(Mapper.Context context) throws IOException, InterruptedException {
            if (context.getInputSplit() instanceof FileSplit){
                this.path = ((FileSplit) context.getInputSplit()).getPath().toString();
            } else {
                this.path = context.getInputSplit().toString();
            }
            URI[] localPaths = context.getCacheFiles();
            parseStopWord(localPaths[0]);
        }

        private void parseStopWord(URI pathURI){
            try {
                BufferedReader bf = new BufferedReader(new FileReader(new File(pathURI.getPath()).getName()));
                String word;
                while((word=bf.readLine()) != null){
                    stopwords.add(word);
                }
            }
            catch(IOException ioe) {
                System.err.println("Error when reading stopword file");
            }
        }
        public void map(Object key, Text sentence, Context context)
            throws IOException, InterruptedException{
                StringTokenizer itr = new StringTokenizer(sentence.toString().replaceAll("\\p{Punct}", "").toLowerCase());
                while(itr.hasMoreTokens()){
                    String nt=itr.nextToken();
                    if (!stopwords.contains(nt)){
                        word.set(nt);
                        context.write(word,one);
                    }
                }
        }
    }
    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable>{
        public void reduce(Text key, Iterable<IntWritable> iterators, Context context)
            throws IOException,InterruptedException{
                int count = 0;
                for (IntWritable itr: iterators){
                    count += itr.get();
                }
                context.write(key,new IntWritable(count));
            }
    }

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf,"word count");
        for (int i = 0; i < args.length; i += 1) {
            if ("-skip".equals(args[i])) {
              i += 1;
              job.addCacheFile(new Path(args[i]).toUri());
            }
          }
        job.setJarByClass(WordCount.class);
        job.setMapperClass(Map.class);
        job.setCombinerClass(Reduce.class);
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
