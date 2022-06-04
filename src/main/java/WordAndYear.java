import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

class WordAndYear implements WritableComparable<WordAndYear> {
    Text word_1;
    Text word_2;
    IntWritable decade;

    WordAndYear() {
        this.word_1 = new Text("");
        this.word_2 = new Text("");
        this.decade = new IntWritable(0);
    }

    WordAndYear(String word_1, String word_2, int year) {
        this.word_1 = new Text(word_1);
        this.word_2 = new Text(word_2);
        this.decade = new IntWritable(year - (year%10));
    }

    public void readFields(DataInput in) throws IOException {
        word_1.readFields(in);
        word_2.readFields(in);
        decade.readFields(in);
    }

    public void write(DataOutput out) throws IOException {
        word_1.write(out);
        word_2.write(out);
        decade.write(out);
    }

    public int compareTo(WordAndYear other) {
        int ret = (int) (decade.get() - other.decade.get());
        if (ret == 0)
            ret = (int) (word_1.compareTo(other.word_1));
        if (ret == 0)
            ret = (int) (word_2.compareTo(other.word_2));
        return ret;
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

}