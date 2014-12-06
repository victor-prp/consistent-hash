package victor.prp.consistent.hash;

import java.util.*;

import static java.util.stream.IntStream.rangeClosed;

/**
 * @author victorp
 */
public class ConsistentHash {
    private int bucketsCount;
    private SortedMap<Long,String> virtualToRealNode = new TreeMap<>();
    private HashFunction hashFunction = DefaultHashFunction.KETAMA_HASH;

    public ConsistentHash(int bucketsCount, Set<String> nodes) {
        this.bucketsCount = bucketsCount;
        nodes.forEach(this::addNode);
    }

    public void addNode(String nodeName){
        rangeClosed(0, bucketsCount)
                .forEach(bucketNumber -> {
                    virtualToRealNode.put(hash((nodeName + bucketNumber)), nodeName);
                });
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
