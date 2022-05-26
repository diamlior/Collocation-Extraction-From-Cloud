import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.StringTokenizer;

public class MapReducer2 {

    public static class Mapper2 extends Mapper<Object, Text, WordAndCounter, IntWritable>{
        String word = "";
        int countFirst = 0;

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            StringTokenizer st = new StringTokenizer(value.toString());
            int index = 0;
            int year = -1;
            int countCouple = 0;
            String firstWord = "";
            String secondWord = "";
            String temp = "";
            boolean isStar = false;

            while (st.hasMoreTokens()) {
                temp = st.nextToken();
                temp = temp.replaceAll("\\u0000", "");
                if (index == 0){
                    firstWord = temp;
                }
                else if (index == 1){
                    secondWord = temp;
                    if (secondWord.equals("*")){
                        word = firstWord;
                        isStar = true;
                    } else {
                        isStar = false;
                    }
                }
                else if (index == 2){
                    year = Integer.parseInt(temp);
                }
                else if (index == 3) {
                    if (isStar){
                        countFirst = Integer.parseInt(temp);
                        context.write(new WordAndCounter(firstWord, "*", year, countFirst), new IntWritable(countFirst));
                        return;
                    }
                    countCouple = Integer.parseInt(temp);
                    context.write(new WordAndCounter(secondWord, firstWord, year, countFirst), new IntWritable(countCouple));
                }
                index++;
            }
        }
    }

    public static class Reducer2
            extends Reducer<WordAndCounter, IntWritable, WordAndCounter, IntWritable> {
        private static WordYearResultsQueue queue = new WordYearResultsQueue(20);
        private IntWritable result = new IntWritable();
        private Text word = new Text();

        public void reduce(WordAndCounter key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            if(key.getSecondWord().contains("*"))
                return;
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            int decade = key.getDecade();
            String firstWord = key.getFirstWord();
            String secondWord = key.getSecondWord();
            queue.insert(new WordYearResult(secondWord, firstWord, decade, sum));
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            super.cleanup(context);
            WordYearResult head = queue.remove();
            while(head != null){
                context.write(new WordAndCounter(head.word_1, head.word_2, head.decade, head.result), new IntWritable(head.result));
                head = queue.remove();
            }
        }
    }

}
