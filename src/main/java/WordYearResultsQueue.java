public class WordYearResultsQueue {

    /*
    This priority queue will be used to hold the top-100 collocations for each decade.
    This queue remains sorted at all time.
     */
    private class WordYearResultsNode{
        public WordYearResult data;
        public WordYearResultsNode next;

        public WordYearResultsNode(WordYearResult data){
            this.data = data;
            this.next = null;
        }
    }

    public int size;
    public int index;
    public WordYearResultsNode head;

    public WordYearResultsQueue(int size){
        this.size = size;
        this.index = 0;
        this.head = null;
    }

    public void insert(WordYearResult data){
        if(this.head == null){
            this.head = new WordYearResultsNode(data);
            index++;
        }
        else if(this.head.data.result < data.result){
            WordYearResultsNode temp = this.head;
            while(temp.next != null && temp.next.data.result < data.result) // Iterate until end of queue or bigger node
                temp = temp.next;
            WordYearResultsNode next = temp.next;
            WordYearResultsNode newNode = new WordYearResultsNode(data);
            temp.next = newNode;
            newNode.next = next;
            index++;
        }
        else if(index < size){
            WordYearResultsNode newNode = new WordYearResultsNode(data);
            newNode.next = this.head;
            this.head = newNode;
            index++;
        }
        if(index > size)
            remove();
    }

    public WordYearResult remove(){
        WordYearResultsNode first = head;
        if(first == null)
            return null;
        this.head = head.next;
        index--;
        return first.data;
    }

    public static void main(String[] args) {
        WordYearResultsQueue a = new WordYearResultsQueue(7);
        a.insert(new WordYearResult("a", "b", 2, 3));
        a.insert(new WordYearResult("a", "b", 2, 5));
        a.insert(new WordYearResult("a", "b", 2, 6));
        a.insert(new WordYearResult("a", "b", 2, 7));
        a.insert(new WordYearResult("a", "b", 2, 8));
        a.insert(new WordYearResult("a", "b", 2, 0));
        a.insert(new WordYearResult("a", "b", 2, 1));
        a.insert(new WordYearResult("a", "b", 2, -3));
        a.insert(new WordYearResult("a", "b", 2, -5));
        a.insert(new WordYearResult("a", "b", 2, 10000));
        a.insert(new WordYearResult("a", "b", 2, 10000));
        System.out.println();
    }
}
