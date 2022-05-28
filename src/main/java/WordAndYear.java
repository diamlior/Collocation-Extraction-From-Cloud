import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

class WordAndYear implements WritableComparable<WordAndYear> {
    String word_1;
    String word_2;
    int decade;

    WordAndYear() {
        this.word_1 = "";
        this.word_2 = "";
        this.decade = -1;
    }

    WordAndYear(String word_1, String word_2, int year) {
        this.word_1 = word_1;
        this.word_2 = word_2;
        this.decade = year - (year%10);
    }

    public void readFields(DataInput in) throws IOException {
        this.word_1 = in.readLine();
        this.word_2 = in.readLine();
        this.decade = in.readInt();
    }

    public void write(DataOutput out) throws IOException {
        out.writeChars(word_1 + "\n");
        out.writeChars(word_2 + "\n");
        out.writeInt(decade);
    }

    public int compareTo(WordAndYear other) {
        int ret = (int) (decade - other.decade);
        if (ret == 0)
            ret = (int) (word_1.compareTo(other.word_1));
        if (ret == 0)
            ret = (int) (word_2.compareTo(other.word_2));
        return ret;
    }

    public String toString(){
        return String.format("%s\t%s\t%d", this.word_1, this.word_2, this.decade);
    }

    public String getFirstWord() {
        return word_1;
    }

    public String getSecondWord() {
        return word_2;
    }

    public int getDecade() {
        return decade;
    }

}