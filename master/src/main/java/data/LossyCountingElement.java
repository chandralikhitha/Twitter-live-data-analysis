package data;

/**
 * Created by ctebbe
 */
public class LossyCountingElement implements Comparable<LossyCountingElement> {

    public final String element;
    public int frequency;
    public int delta;

    public LossyCountingElement(String e, int delta) {
        element = e;
        frequency = 1;
        this.delta = delta;
    }

    public void increment() {
        frequency++;
    }

    public String toString() {
        return element +":"+ frequency +":"+ delta;
    }

    @Override
    public int compareTo(LossyCountingElement that) {
        final int BEFORE=-1, EQUAL=0, AFTER=1;
        if(this == that) return EQUAL;
        else if(this.frequency < that.frequency) return BEFORE;
        else if(this.frequency > that.frequency) return AFTER;
        return EQUAL;
    }
}
