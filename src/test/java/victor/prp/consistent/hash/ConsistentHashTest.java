package victor.prp.consistent.hash;

import org.junit.Assert;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * @author victorp
 */
public class ConsistentHashTest {


    @Test
    public void validateRebalancingWhenNodeIsAdded(){
        int NODES_COUNT = 10;
        int BUCKETS_COUNT =  100;
        int KEYS_COUNT = 1000000;

        final Set<String> keys = createRandomKeys(KEYS_COUNT);

        /**
         * Phase 1
         * Simulate with random nodes and random keys
         */
        final Map<String,AtomicInteger> nodesPhase1 = initNodesRandomly(NODES_COUNT);
        Map<String,Set<String>> nodes2KeysPhase1 = simulate(BUCKETS_COUNT, nodesPhase1, keys);

        /**
         * Phase 2
         * Simulate with same nodes from phase 1 + "new-node" and with same keys from phase 1
         */
        final Map<String,AtomicInteger> nodesPhase2 = initNodes(nodesPhase1.keySet(),"new-node");
        Map<String,Set<String>> nodes2KeysPhase2 = simulate(BUCKETS_COUNT, nodesPhase2, keys);

        /**
         * Phase 3
         * Validate re-balancing was as expected
         */
        long movedKeysCountResult = calculatedMovedKeys(nodes2KeysPhase1, nodes2KeysPhase2);
        System.out.println("moved (rebalanced) keys count: " + movedKeysCountResult);


    }

    private long calculatedMovedKeys(Map<String, Set<String>> nodes2KeysPhase1, Map<String, Set<String>> nodes2KeysPhase2) {
        AtomicLong movedKeysCount = new AtomicLong(0);
        nodes2KeysPhase1.forEach((node,keysInNode)->{
             long movedCount =
                     keysInNode.stream()
                        .filter(key -> !match(nodes2KeysPhase2, node, key))
                        .count();
             movedKeysCount.addAndGet(movedCount);
        });

        return movedKeysCount.get();
    }

    private Map<String, AtomicInteger> initNodes(Set<String> nodes, String... moreNodes) {
        Set<String> nodesToInit = new HashSet<>(nodes);
        nodesToInit.addAll(Arrays.asList(moreNodes));
        return nodesToInit.stream().collect(Collectors.toMap(node->node,node->new AtomicInteger()));
    }

    private Map<String,Set<String>> simulate(int bucketCount, Map<String, AtomicInteger> nodes, Set<String> keys) {
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

    private static Map<String, Set<String>> initNodes2Keys(Set<String> nodes) {
        final Map<String,Set<String>> node2keys = new HashMap<>(nodes.size());
        nodes.forEach(node -> node2keys.put(node,new HashSet<>()));
        return node2keys;
    }

    private static Set<String> createRandomKeys(int keysCount) {
        return
            IntStream.rangeClosed(1, keysCount)
                .mapToObj(sequence -> UUID.randomUUID().toString())
                .collect(Collectors.toSet());
    }

    private boolean match(Map<String, Set<String>> node2keys_after, String node, String key) {
        return node2keys_after.get(node).contains(key);
    }

    @Test
    public void validateDistribution(){
        int NODES_COUNT = 100;
        int BUCKETS_COUNT =  10000;
        int KEYS_COUNT = 1000000;

        final Map<String,AtomicInteger> nodes = initNodesRandomly(NODES_COUNT);

        ConsistentHash consistentHash = new ConsistentHash(BUCKETS_COUNT,nodes.keySet());

        IntStream.rangeClosed(1,KEYS_COUNT)
                .forEach(k->{
                    String randomKey  = UUID.randomUUID().toString();
                    String node = consistentHash.calculateNode(randomKey);
                    Assert.assertTrue("algo returned non existing node",nodes.containsKey(node));
                    nodes.get(node).incrementAndGet();
                });

        nodes.forEach((node,count)-> {
            System.out.println("node: " + node + " count: " + count.get());
        });


        int sum = nodes.values()
                .stream()
                .reduce((l, r) -> new AtomicInteger(l.get() + r.get())).get().get();
        System.out.println("sum: " + sum);

        int expectedCountPerNode = KEYS_COUNT / NODES_COUNT;
        nodes.forEach((node,count)-> {
            Assert.assertFalse("Count is too big in node: " + node, (double) count.get() > ((double) expectedCountPerNode) * 1.05);
            Assert.assertFalse("Count is too small in node: "+node,(double)count.get() < ((double)expectedCountPerNode)*0.95);

        });

    }


    private Map<String,AtomicInteger> initNodesRandomly(int nodesCount) {
        final String nodePrototype = "node-";
        final Map<String,AtomicInteger> result = new HashMap<>();
        final Random random = new Random();
        IntStream.rangeClosed(1,nodesCount)
                .map(n -> random.nextInt())
                .forEach(r -> result.put(nodePrototype + r, new AtomicInteger(0)));
        return result;
    }
}
