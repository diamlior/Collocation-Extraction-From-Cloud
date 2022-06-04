import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

public class MapReducer1 {
    public static class TokenizerMapper
            extends Mapper<Object, Text, WordAndYear, IntWritable> {

        private Set<String> stopWords = new HashSet<>();
        private String lang = "";
        private String regx = "";
        private final static IntWritable one = new IntWritable(1);

        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();

            try {
                lang = conf.get("lang");
            } catch (Exception e){
                e.printStackTrace();
            }
            if (lang.equals("heb")){
                regx = "[\\u0590-\\u05fe]+";
            } else {
                lang = "[a-zA-Z]+";
            }
            try {
                for (String word : conf.get("stop.words").split("\n")) {
                    word = word.replace("\r", "");
                    stopWords.add(word);
                }
            } catch (Exception e){
                e.printStackTrace();
            }
        }

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            StringTokenizer st = new StringTokenizer(value.toString());
            int index = 0;
            int year = -1;
            int count = 0;
            String firstWord = "";
            String secondWord = "";
            String temp = "";
            while (st.hasMoreTokens()) {
                temp = st.nextToken();
//                temp = temp.replaceAll("[^\\w]", "");
                if(index == 0){
                    if (temp.length() < 2 || !temp.matches(this.regx) || stopWords.contains(temp)){
                        return;
                    }
                    firstWord = temp;
                }
                if(index == 1){
                    if (temp.length() < 2 || !temp.matches(this.regx) || stopWords.contains(temp)){
                        return;
                    }
                    secondWord = temp;
                }
                if(index == 2){
                    try{
                        year = Integer.parseInt(temp);
                    }
                    catch (Exception e){
                        return;
                    }
                }
                if(index == 3){
                    try {
                        count = Integer.parseInt(temp);
                    } catch (Exception e){
                        return;
                    }
                    context.write(new WordAndYear(firstWord, secondWord, year), new IntWritable(count));
                    context.write(new WordAndYear(firstWord, "*", year), new IntWritable(count));
                    context.write(new WordAndYear(secondWord, "*", year), new IntWritable(count));
                    context.write(new WordAndYear("*", "*", year), new IntWritable(count));
                }
                if(index > 3)
                    return;
                else
                    index++;
            }

        }

    }

    public static class IntSumReducer
            extends Reducer<WordAndYear, IntWritable, WordAndYear, IntWritable> {
        private IntWritable result = new IntWritable();
        private Text word = new Text();

        public void reduce(WordAndYear key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static class DecadePartitioner1 extends Partitioner<WordAndYear, IntWritable> {
        @Override
        public int getPartition(WordAndYear key, IntWritable value, int i) {
            return key.getDecade()/10 % i;
        }
    }
}
