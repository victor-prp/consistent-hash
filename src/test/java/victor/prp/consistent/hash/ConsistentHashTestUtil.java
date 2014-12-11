package victor.prp.consistent.hash;

import org.junit.Assert;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * @author victorp
 */
public class ConsistentHashTestUtil {

    public static long calculatedMovedKeys(Map<String, Set<String>> nodes2KeysLeft, Map<String, Set<String>> nodes2KeysRight) {
        AtomicLong movedKeysCount = new AtomicLong(0);
        nodes2KeysLeft.forEach((node, keysInNode) -> {
            long movedCount =
                    keysInNode.stream()
                            .filter(key -> !match(nodes2KeysRight, node, key))
                            .count();
            movedKeysCount.addAndGet(movedCount);
        });

        return movedKeysCount.get();
    }

    public static Map<String, AtomicInteger> initNodes(Set<String> nodes, String... moreNodes) {
        Set<String> nodesToInit = new HashSet<>(nodes);
        nodesToInit.addAll(Arrays.asList(moreNodes));
        return nodesToInit.stream().collect(Collectors.toMap(node -> node, node -> new AtomicInteger()));
    }

    public static Map<String,Set<String>> simulate(int bucketCount, Map<String, AtomicInteger> nodes, Set<String> keys) {
        final Map<String,Set<String>> node2keys = initNodes2Keys(nodes.keySet());
        ConsistentHash consistentHash = new ConsistentHash(bucketCount,nodes.keySet());

        keys.stream()
                .forEach(key -> {
                    String node = consistentHash.calculateNode(key);
                    Assert.assertTrue("algo returned non existing node", nodes.containsKey(node));
                    nodes.get(node).incrementAndGet();
                    node2keys.get(node).add(key);
                });

        nodes.forEach((node,count)-> {
            System.out.println("node: " + node + " count: " + count.get() + " key size: " + node2keys.get(node).size());

        });

        int sum = nodes.values()
                .stream()
                .reduce((l, r) -> new AtomicInteger(l.get() + r.get())).get().get();
        System.out.println("sum: " + sum);
        return node2keys;
    }

    public static Map<String, Set<String>> initNodes2Keys(Set<String> nodes) {
        final Map<String,Set<String>> node2keys = new HashMap<>(nodes.size());
        nodes.forEach(node -> node2keys.put(node,new HashSet<>()));
        return node2keys;
    }

    public static Set<String> createRandomKeys(int keysCount) {
        return
                IntStream.rangeClosed(1, keysCount)
                        .mapToObj(sequence -> UUID.randomUUID().toString())
                        .collect(Collectors.toSet());
    }

    public static boolean match(Map<String, Set<String>> node2keys, String node, String key) {
        if (node2keys.get(node)== null){
            return false;
        }
        return node2keys.get(node).contains(key);
    }



    public  static Map<String,AtomicInteger> initNodesRandomly(int nodesCount) {
        final String nodePrototype = "node-";
        final Map<String,AtomicInteger> result = new HashMap<>();
        final Random random = new Random();
        IntStream.rangeClosed(1,nodesCount)
                .map(n -> random.nextInt())
                .forEach(r -> result.put(nodePrototype + r, new AtomicInteger(0)));
        return result;
    }
}
