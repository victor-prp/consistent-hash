package victor.prp.consistent.hash;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.stream.IntStream.rangeClosed;

/**
 * @author victorp
 */
public class ConsistentHash {
    private int bucketsCount;
    private SortedMap<Long,String> virtualToRealNode = new TreeMap<>();
    private HashFunction hashFunction = DefaultHashFunction.KETAMA_HASH;

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
                    String previousMapping = virtualToRealNode.put(hash((nodeName + bucketNumber)), nodeName);
                    if (previousMapping != null){
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
        SortedMap<Long,String> tailMap = virtualToRealNode.tailMap(hash);
        if (!tailMap.isEmpty()){
            virtualNode = tailMap.firstKey();
        }
        return virtualToRealNode.get(virtualNode);
    }

}
