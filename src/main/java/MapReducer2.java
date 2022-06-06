import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.StringTokenizer;

public class MapReducer2 {

    public static class Mapper2 extends Mapper<Object, Text, WordAndCounter, DoubleWritable>{
        String word = "";
        double countFirst = 0;

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            StringTokenizer st = new StringTokenizer(value.toString());
            int index = 0;
            int year = -1;
            double countCouple = 0;
            String firstWord = "";
            String secondWord = "";
            String temp = "";
            boolean isStar = false;

            while (st.hasMoreTokens()) {
                temp = st.nextToken();
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
                        countFirst = Double.parseDouble(temp);
                        context.write(new WordAndCounter(firstWord, "*", year, countFirst), new DoubleWritable(countFirst));
                        return;
                    }
                    countCouple = Double.parseDouble(temp);
                    context.write(new WordAndCounter(secondWord, firstWord, year, countFirst), new DoubleWritable(countCouple));
                }
                index++;
            }
        }
    }

    public static class Reducer2
            extends Reducer<WordAndCounter, DoubleWritable, WordAndCounter, DoubleWritable> {

        private WordYearResultsQueue queue = new WordYearResultsQueue(100);
        private double leftCounter = -1;
        private double N = -1;
        private double current_decade = -1;

        public double getLogValue(double sumOfBoth, double sumOfLeft, double sumOfRight, double total) {
            double c12 = sumOfBoth;
            double c1 = sumOfLeft;
            double c2 = sumOfRight;

            if (c1 < 4 || c2 < 4 || c1 == c12 || c2 == c12) // Ignore words that appeared less than 4 times or only part of the pair
                return 0;

            double N = total;
            double p = c2 / (N + 1);
            double p1 = c12 / c1;
            double p2 = (c2 - c12) / (N - c1);

            return getLValue(c12, c1, p) +
                    getLValue(c2 - c12, N - c1, p) -
                    getLValue(c12, c1, p1) -
                    getLValue(c2 - c12, N - c1, p2);
        }

        public double getLValue(double k, double n, double x) {
            return k * Math.log(x) + (n - k) * Math.log(1 - x);
        }

        public void reduce(WordAndCounter key, Iterable<DoubleWritable> values,
                           Context context
        ) throws IOException, InterruptedException {

            if (key.getSecondWord().contains("*")) {
                if (key.getFirstWord().contains("*")){
                    N = key.getRightword_counter();
                    return;
                }
                else
                    try {
                        leftCounter = key.getRightword_counter();
                        return;
                    } catch (Exception e) {
                        leftCounter = 0;
                        return;
                    }
            }

            double sumOfBoth = 0;
            for (DoubleWritable val : values) {
                sumOfBoth += val.get();
            }
            double sum = -2 * getLogValue(sumOfBoth, leftCounter, key.getRightword_counter(), N);
            if(sum != sum) // If sum is NAN
                sum = Integer.MIN_VALUE;
            int decade = key.getDecade();
            String firstWord = key.getFirstWord();
            String secondWord = key.getSecondWord();

            // Check if decade wasn't initialized yet
            if (current_decade == -1)
                current_decade = decade;

            // If it's a new decade, flush the queue of the previous decade
            if (current_decade != decade) {
                WordYearResult head = queue.remove();
                while (head != null) {
                    context.write(new WordAndCounter(head.word_1, head.word_2, head.decade, leftCounter), new DoubleWritable(head.result));
                    head = queue.remove();
                }
                current_decade = decade;
            }

            // Add value to queue
            queue.insert(new WordYearResult(secondWord, firstWord, decade, sum));
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            super.cleanup(context);

            // Flush the queue of the last decade
            WordYearResult head;
            head = queue.remove();
            while(head != null){
                context.write(new WordAndCounter(head.word_1, head.word_2, head.decade, leftCounter), new DoubleWritable(head.result));
                head = queue.remove();
            }
        }
    }

    public static class DecadePartitioner2 extends Partitioner<WordAndCounter, DoubleWritable> {
        @Override
        public int getPartition(WordAndCounter key, DoubleWritable value, int i) {
            return key.getDecade()/10 % i;
        }
    }
}
