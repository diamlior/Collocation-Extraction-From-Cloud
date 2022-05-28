import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.StringTokenizer;

public class MapReducer1 {
    public static class TokenizerMapper
            extends Mapper<Object, Text, WordAndYear, IntWritable> {

        private final static IntWritable one = new IntWritable(1);

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
                temp = temp.replaceAll("[^\\w]", "");
                if(index == 0){
                    if (!temp.matches("[a-zA-Z]+")){
                        return;
                    }
                    firstWord = temp;
                }
                if(index == 1){
                    if (!temp.matches("[a-zA-Z]+")){
                        return;
                    }
                    secondWord = temp;
                }
                if(index == 2){
                    try{
                        year = Integer.parseInt(temp);
                    }
                    catch (Exception e){
                        continue;
                    }
                }
                if(index == 3){
                    try {
                        count = Integer.parseInt(temp);
                    } catch (Exception e){
                        continue;
                    }
                    context.write(new WordAndYear(firstWord, secondWord, year), new IntWritable(count));
                    context.write(new WordAndYear(firstWord, "*", year), new IntWritable(count));
                    context.write(new WordAndYear(secondWord, "*", year), new IntWritable(count));
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
}
