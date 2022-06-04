import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

class WordYearFinalResult implements WritableComparable<WordYearFinalResult> {
    Text word_1;
    Text word_2;
    IntWritable decade;
    DoubleWritable result;

    WordYearFinalResult() {
        this.word_1 = new Text("");
        this.word_2 = new Text("");
        this.decade = new IntWritable(-1);
        this.result = new DoubleWritable(-1);
    }

    public WordYearFinalResult(String word_1, String word_2, int decade, double result) {
        this.word_1 = new Text(word_1);
        this.word_2 = new Text(word_2);
        this.result = new DoubleWritable(result);
        this.decade = new IntWritable(decade);
    }

    @Override
    public int compareTo(WordYearFinalResult other) {
        if(other == null)
            return 1;
        int ret = decade.get() - other.getDecade();
        if(ret != 0)
            return ret;
        if(result.get() == other.getResult())
            return 0;
        if(result.get() > other.getResult())
            return -1;
        return 1;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        word_1.write(out);
        word_2.write(out);
        result.write(out);
        decade.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        word_1.readFields(in);
        word_2.readFields(in);
        result.readFields(in);
        decade.readFields(in);
    }

    public String toString(){
        return String.format("%s\t%s\t%d", this.word_1, this.word_2, this.decade.get());
    }

    public String getFirstWord() {
        return word_1.toString();
    }

    public String getSecondWord() {
        return word_2.toString();
    }

    public int getDecade() {
        return decade.get();
    }

    public double getResult() { return result.get(); }
}