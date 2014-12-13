package victor.prp.consistent.hash;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.stream.IntStream.rangeClosed;

/**
 * @author victorp
 */
public class ConsistentHash {
    private int bucketsCount;
    private SortedMap<Long,SortedSet<String>> virtualToRealNode = new TreeMap<>();
    private HashFunction hashFunction = MD5Hash.INSTANCE;

    public ConsistentHash(int bucketsCount, String... nodes) {
        this(bucketsCount,CollectionsUtil.asSet(nodes));
    }

    public ConsistentHash(int bucketsCount, Set<String> nodes) {
        this.bucketsCount = bucketsCount;
        nodes.forEach(this::addNode);
    }


    public int addNode(String nodeName){
        AtomicInteger collisionsCount = new AtomicInteger();
        rangeClosed(0, bucketsCount)
                .forEach(bucketNumber -> {
                    long hash = hash((nodeName + bucketNumber));
                    SortedSet<String> previousMapping = virtualToRealNode.get(hash);
                    if (previousMapping == null){
                        SortedSet<String> newMapping = new TreeSet<>();
                        newMapping.add(nodeName);
                        virtualToRealNode.put(hash,newMapping);
                    }else{
                        previousMapping.add(nodeName);
                        collisionsCount.incrementAndGet();
                    }
                });
        return collisionsCount.get();
    }

    public void removeNode(String nodeName){
        rangeClosed(0, bucketsCount)
                .forEach(bucketNumber -> {
                    virtualToRealNode.remove(hash((nodeName + bucketNumber)));
                });
    }

    private long hash(String key){
        return hashFunction.apply(key);
    }

    public String calculateNode(String key){
        long hash = hash(key);
        long virtualNode = virtualToRealNode.firstKey();
        SortedMap<Long,SortedSet<String>> tailMap = virtualToRealNode.tailMap(hash);
        if (!tailMap.isEmpty()){
            virtualNode = tailMap.firstKey();
        }
        return virtualToRealNode.get(virtualNode).first();
    }

}
