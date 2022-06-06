import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

public class MapReducer1 {
    public static class TokenizerMapper
            extends Mapper<Object, Text, WordAndYear, DoubleWritable> {

        private Set<String> stopWords = new HashSet<>();
        private String lang = "";
        private String regx = "";
        private final static DoubleWritable one = new DoubleWritable(1);

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
                regx = "[a-zA-Z]+";
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
            double count = 0;
            String firstWord = "";
            String secondWord = "";
            String temp = "";
            while (st.hasMoreTokens()) {
                temp = st.nextToken();
                if (this.lang.equals("eng")) {
                    temp = temp.replaceAll("[\\0000]", "");
                }
                if(index == 0){
                    if (temp.length() < 2 || !temp.matches(this.regx) || (this.lang.equals("eng") && stopWords.contains(temp.toLowerCase()))
                        || (this.lang.equals("heb") && stopWords.contains(temp))){
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
                        count = Double.parseDouble(temp);
                    } catch (Exception e){
                        return;
                    }
                    context.write(new WordAndYear(firstWord, secondWord, year), new DoubleWritable(count));
                    context.write(new WordAndYear(firstWord, "*", year), new DoubleWritable(count));
                    context.write(new WordAndYear(secondWord, "*", year), new DoubleWritable(count));
                    context.write(new WordAndYear("*", "*", year), new DoubleWritable(count));
                }
                if(index > 3)
                    return;
                else
                    index++;
            }

        }

    }

    public static class IntSumReducer
            extends Reducer<WordAndYear, DoubleWritable, WordAndYear, DoubleWritable> {
        private DoubleWritable result = new DoubleWritable();
        private Text word = new Text();

        public void reduce(WordAndYear key, Iterable<DoubleWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            double sum = 0;
            for (DoubleWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static class DecadePartitioner1 extends Partitioner<WordAndYear, DoubleWritable> {
        @Override
        public int getPartition(WordAndYear key, DoubleWritable value, int i) {
            return key.getDecade()/10 % i;
        }
    }
}
