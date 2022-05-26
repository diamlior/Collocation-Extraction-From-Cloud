import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

class WordYearResult implements WritableComparable<WordYearResult> {
    String word_1;
    String word_2;
    int decade;
    int result;

    WordYearResult() {
        this.word_1 = "";
        this.word_2 = "";
        this.decade = -1;
        this.result = -1;
    }

    public WordYearResult(String word_1, String word_2, int decade, int result) {
        this.word_1 = word_1;
        this.word_2 = word_2;
        this.result = result;
        this.decade = decade;
    }

    @Override
    public int compareTo(WordYearResult other) {
        if(other == null)
            return 1;
        return result - other.result;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeChars(word_1 + "\n");
        out.writeChars(word_2 + "\n");
        out.writeInt(result);
        out.writeInt(decade);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.word_1 = in.readLine();
        this.word_2 = in.readLine();
        this.result = in.readInt();
        this.decade = in.readInt();
    }

    public String toString(){
        return String.format("%s\t%s\t%d\t%d", this.word_1, this.word_2, this.decade, this.result);
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

    public int getResult() { return result; }
}