import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.StringTokenizer;

public class MapReducer3 {

    public static class Mapper3 extends Mapper<Object, Text, WordYearFinalResult, DoubleWritable> {
        double logResult = 0;

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            StringTokenizer st = new StringTokenizer(value.toString());
            int index = 0;
            int year = -1;
            String firstWord = "";
            String secondWord = "";
            String temp = "";

            while (st.hasMoreTokens()) {
                temp = st.nextToken();
                if (index == 0) {
                    firstWord = temp;
                } else if (index == 1) {
                    secondWord = temp;
                } else if (index == 2) {
                    year = Integer.parseInt(temp);
                } else if (index == 3) {
                    logResult = Double.parseDouble(temp);
                    context.write(new WordYearFinalResult(firstWord, secondWord, year, logResult), new DoubleWritable(logResult));
                    return;
                }
                index++;
            }

        }
    }


    public static class Reducer3
            extends Reducer<WordYearFinalResult, DoubleWritable, WordYearFinalResult, DoubleWritable> {

        public void reduce(WordYearFinalResult key, Iterable<DoubleWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            context.write(key, new DoubleWritable(key.getResult()));
        }
    }
}
