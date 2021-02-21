package data;

import com.google.common.collect.Lists;

import java.io.Serializable;
import java.util.Iterator;
import java.util.List;

/**
 * Created by ctebbe
 */
public class LossyCountingDataStructure implements Serializable {

    private int n;
    private int currBucket;
    private int sizeBucket;
    private final List<LossyCountingElement> bucket = Lists.newArrayList();

    public LossyCountingDataStructure(int size) {
        sizeBucket = size;
        n = 0;
        currBucket = 1;
    }

    public synchronized void insert(String element) {
        findOrInsert(element);
        if(++n % sizeBucket == 0) { // bucket is full
            deletePhase();
            currBucket++;
        }
    }

    private void findOrInsert(String hashtag) {
        LossyCountingElement element = null;
        for(LossyCountingElement e : bucket) {
            if(e.element.equalsIgnoreCase(hashtag)) {
                e.increment();
            }
        }

        if(element == null) {
            element = new LossyCountingElement(hashtag, currBucket-1);
            bucket.add(element);
        }
    }

    /*
        remove elements that satisfy freq + delta <= currBucket
     */
    private void deletePhase() {
        for(Iterator<LossyCountingElement> iter = bucket.iterator(); iter.hasNext();) {
            LossyCountingElement e = iter.next();
            if(e.frequency + e.delta <= currBucket)
                iter.remove();
        }
    }

    public synchronized List<LossyCountingElement> getResults() {
        List<LossyCountingElement> l;
         l = Lists.newArrayList(bucket);
        bucket.clear();
        return l;
    } 

    public static void main(String[] args) {
        LossyCountingDataStructure buckets = new LossyCountingDataStructure(5);
        buckets.insert("1");
        buckets.insert("2");
        buckets.insert("4");
        buckets.insert("3");
        buckets.insert("4");

        buckets.insert("3");
        buckets.insert("4");
        buckets.insert("5");
        buckets.insert("4");
        buckets.insert("6");

        buckets.insert("7");
        buckets.insert("3");
        buckets.insert("3");
        buckets.insert("6");
        buckets.insert("1");

        buckets.insert("1");
        buckets.insert("3");
        buckets.insert("2");
        buckets.insert("4");
        buckets.insert("7");
        System.out.println(buckets.getResults());
    }
}
