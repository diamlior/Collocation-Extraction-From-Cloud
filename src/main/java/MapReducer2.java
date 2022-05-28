import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
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
            int countCouple = 0;
            String firstWord = "";
            String secondWord = "";
            String temp = "";
            boolean isStar = false;

            while (st.hasMoreTokens()) {
                temp = st.nextToken();
                temp = temp.replaceAll("[^\\w*]", "");
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
                        countFirst = (double) (Integer.parseInt(temp));
                        context.write(new WordAndCounter(firstWord, "*", year, countFirst), new DoubleWritable(countFirst));
                        return;
                    }
                    countCouple = Integer.parseInt(temp);
                    context.write(new WordAndCounter(secondWord, firstWord, year, countFirst), new DoubleWritable(countCouple));
                }
                index++;
            }
        }
    }

    public static class Reducer2
            extends Reducer<WordAndCounter, DoubleWritable, WordAndCounter, DoubleWritable> {

        // <Word2, *, CounterOf1, decade> Counter
        // <Word1, Word2, CounterOf1, decade> <Word1Word2Counter>
        private static HashMap<Integer, WordYearResultsQueue> queueMap = new HashMap<>();
        WordYearResultsQueue queue = new WordYearResultsQueue(100);
        private DoubleWritable result = new DoubleWritable();
        private Text word = new Text();
        private int leftCounter = -1;
        private int N = 500000; // TODO: Find N
        private int current_decade = -1;

        public double getLogValue(int sumOfBoth, int sumOfLeft, int sumOfRight, int total) {
            int c12 = sumOfBoth;
            int c1 = sumOfLeft;
            int c2 = sumOfRight;

            c1++; // TODO: remove this line

            // If a word exists only with the other one, it's best collocation
            if(c1 == c12 || c2 == c12)
                return Integer.MIN_VALUE / 2;

            int N = total;
            double p = (double) c2 / N;
            double p1 = (double) c12 / c1;
            double p2 = (double) (c2 - c12) / (N - c1);

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
                try {
                    leftCounter = (int) key.getRightword_counter();
                    return;
                } catch (Exception e) {
                    leftCounter = 0;
                    return;
                }
            }

            int sumOfBoth = 0;
            for (DoubleWritable val : values) {
                sumOfBoth += val.get();
            }
            double sum = -2 * getLogValue(sumOfBoth, (int) leftCounter, (int) key.getRightword_counter(), N);
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
