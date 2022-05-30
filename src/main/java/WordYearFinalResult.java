import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

class WordYearFinalResult implements WritableComparable<WordYearFinalResult> {
    String word_1;
    String word_2;
    int decade;
    double result;

    WordYearFinalResult() {
        this.word_1 = "";
        this.word_2 = "";
        this.decade = -1;
        this.result = -1;
    }

    public WordYearFinalResult(String word_1, String word_2, int decade, double result) {
        this.word_1 = word_1;
        this.word_2 = word_2;
        this.result = result;
        this.decade = decade;
    }

    @Override
    public int compareTo(WordYearFinalResult other) {
        if(other == null)
            return 1;
        int ret = decade - other.getDecade();
        if(ret != 0)
            return ret;
        if(result == other.getResult())
            return 0;
        if(result > other.getResult())
            return -1;
        return 1;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeChars(word_1 + "\n");
        out.writeChars(word_2 + "\n");
        out.writeDouble(result);
        out.writeInt(decade);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.word_1 = in.readLine();
        this.word_2 = in.readLine();
        this.result = in.readDouble();
        this.decade = in.readInt();
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

    public double getResult() { return result; }
}