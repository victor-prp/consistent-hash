package victor.prp.consistent.hash;

import org.junit.Assert;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * @author victorp
 */
public class ConsistentHashTestUtil {
    private ConsistentHashTestUtil() {
    }

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

    public static Set<String> initNodes(Set<String> nodes, String... moreNodes) {
        Set<String> nodesToInit = new HashSet<>(nodes);
        nodesToInit.addAll(Arrays.asList(moreNodes));
        return nodesToInit;
    }

    public static Map<String,Set<String>> simulate(ConsistentHash consistentHash, Set<String> nodes, Set<String> keys) {
        final Map<String,Set<String>> node2keys = initNodes2Keys(nodes);

        keys.stream()
                .forEach(key -> {
                    String node = consistentHash.calculateNode(key);
                    Assert.assertTrue("algo returned non existing node", nodes.contains(node));
                    node2keys.get(node).add(key);
                });

        nodes.forEach((node)-> {
            System.out.println("node: " + node + " keys count: " + node2keys.get(node).size());
        });

        AtomicInteger sum = new AtomicInteger();
        node2keys.values().forEach(keysSet -> sum.addAndGet(keysSet.size()));

        System.out.println("sum: " + sum.get());
        return node2keys;
    }


    private static int addNodes(ConsistentHash consistentHash,Stream<String> nodes) {
        AtomicInteger allCollisionsCount = new AtomicInteger(0);
        nodes
            .forEach(node -> {
                int collisionsCount = consistentHash.addNode(node);
                allCollisionsCount.addAndGet(collisionsCount);
            });

        return allCollisionsCount.get();
    }


    public static int addNodesRandomly(ConsistentHash consistentHash, Set<String> nodes) {
        String[] nodesArray = new String[nodes.size()];
        final Random random = new Random();
        nodes.forEach(node -> addNodeToRandomLocation(nodesArray, random, node));
        return addNodes(consistentHash,Arrays.stream(nodesArray));

    }

    private static void addNodeToRandomLocation(String[] nodesArray, Random random, String node) {
        boolean added = false;
        while (!added){
            int index = random.nextInt(nodesArray.length);
            if (nodesArray[index] == null){
                nodesArray[index] = node;
                added = true;
            }
        }
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



    public  static Set<String> initNodesSequentially(int nodesCount) {
        final String nodePrototype = "node-";
        return IntStream.rangeClosed(1,nodesCount)
                    .mapToObj(sequence -> nodePrototype + sequence)
                    .collect(Collectors.toSet());
    }

    public  static Set<String> initNodesRandomly(int nodesCount) {
        final String nodePrototype = "node-";
        final Random random = new Random();
        return IntStream.rangeClosed(1,nodesCount)
                    .mapToObj(sequence -> nodePrototype + random.nextInt())
                    .collect(Collectors.toSet());
    }

    public  static ConsistentHash initConsistentHash(int bucketCount, Set<String> nodes){
        ConsistentHash consistentHash = new ConsistentHash(bucketCount);
        int collisionCount = addNodes(consistentHash, nodes.stream());
        System.out.println("ConsistentHash was created with " + nodes.size()+ " nodes. Collisions during creation: " + collisionCount);
        return consistentHash;
    }


}
