import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

class WordAndCounter implements WritableComparable<WordAndCounter> {

    Text word_1;
    Text word_2;
    DoubleWritable rightword_counter;
    IntWritable decade;

    WordAndCounter() {
        this.word_1 = new Text("");
        this.word_2 = new Text("");
        this.decade = new IntWritable(-1);
        this.rightword_counter = new DoubleWritable(-1);
    }

    public WordAndCounter(String word_1, String word_2, int decade, double rightword_counter) {
        this.word_1 = new Text(word_1);
        this.word_2 = new Text(word_2);
        this.rightword_counter = new DoubleWritable(rightword_counter);
        this.decade = new IntWritable(decade);
    }

    @Override
    public int compareTo(WordAndCounter other) {
        // First compare decades
        int ret = decade.get() - other.decade.get();
        if (ret == 0)
            // Then compare left word
            ret = (int) (word_1.compareTo(other.word_1));
        if (ret == 0)
            // Only then compare right word
            ret = (int) (word_2.compareTo(other.word_2));
        return ret;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        word_1.write(out);
        word_2.write(out);
        rightword_counter.write(out);
        decade.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        word_1.readFields(in);
        word_2.readFields(in);
        rightword_counter.readFields(in);
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

    public double getRightword_counter() { return rightword_counter.get(); }
}
