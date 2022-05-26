import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
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
        private int leftCounter = 0;
        private int p = 9000;

        public double getLogValue(int sumOfBoth, int sumOfLeft, int sumOfRight, int total){
            int c12 = sumOfBoth;
            int c1 = sumOfLeft;
            int c2 = sumOfRight;
            if(c1 == c12 || c2 == c12)
                return 1;
            int N = total;
            double p = (double) c2 / N;
            double p1 = (double) c12 / c1;
            double p2 = (double) (c2 - c12) / (N - c1);

            return Math.log(getLValue(c12, c1, p)) +
                    Math.log(getLValue(c2 - c12, N - c1, p)) -
                    Math.log(getLValue(c12, c1, p1)) -
                    Math.log(getLValue(c2 - c12, N - c1, p2));
        }

        public double getLValue(double k, double n, double x){
            return Math.pow(x, k) * Math.pow((1 - x), (n - k));
        }

        public void reduce(WordAndCounter key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            if(key.getSecondWord().contains("*")) {
                leftCounter = key.getRightword_counter();
                return;
            }
            int sumOfBoth = 0;
            for (IntWritable val : values) {
                sumOfBoth += val.get();
            }
            double sum = getLogValue(sumOfBoth, leftCounter, key.getRightword_counter(), p);
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
                context.write(new WordAndCounter(head.word_1, head.word_2, head.decade, leftCounter), new IntWritable((int) head.result));
                head = queue.remove();
            }
        }
    }

}
