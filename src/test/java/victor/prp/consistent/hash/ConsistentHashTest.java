package victor.prp.consistent.hash;

import org.junit.Assert;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;

/**
 * @author victorp
 */
public class ConsistentHashTest {


    @Test
    public void validateRehashingOnAdd(){
        int NODES_COUNT = 10;
        int BUCKETS_COUNT =  100;
        int KEYS_COUNT = 1000000;

        /**
         * Phase 1
         * Simulate NODES_COUNT with KEYS_COUNT
         */
        final Map<String,AtomicInteger> nodes_init = initNodesRandomly(NODES_COUNT);
        final Map<String,Set<String>> node2keys_init = new HashMap<>();
        final Map<String,Set<String>> node2keys_after = new HashMap<>();

        nodes_init.forEach((node,keysCount)-> {
            node2keys_init.put(node,new HashSet<>());
            node2keys_after.put(node,new HashSet<>());
        });

        ConsistentHash consistentHash = new ConsistentHash(BUCKETS_COUNT,nodes_init.keySet());

        IntStream.rangeClosed(1,KEYS_COUNT)
                .forEach(k->{
                    String randomKey  = UUID.randomUUID().toString();
                    String node = consistentHash.calculateNode(randomKey);
                    Assert.assertTrue("algo returned non existing node",nodes_init.containsKey(node));
                    nodes_init.get(node).incrementAndGet();
                    node2keys_init.get(node).add(randomKey);
                });

        nodes_init.forEach((node,count)-> {
            System.out.println("node: " + node + " count: " + count.get() + " key size: " + node2keys_init.get(node).size());

        });

        int sum = nodes_init.values()
                .stream()
                .reduce((l, r) -> new AtomicInteger(l.get() + r.get())).get().get();
        System.out.println("sum: " + sum);

        System.out.println("adding new node to the system..");


        consistentHash.addNode("new-node");
        node2keys_after.put("new-node",new HashSet<>());

        /**
         * Phase 2
         * Simulate NODES_COUNT + 1 with KEYS_COUNT using the same keys from phase 1
         */
        final Map<String,AtomicInteger> nodes_after = new HashMap<>();
        nodes_after.putAll(nodes_init);
        nodes_after.put("new-node", new AtomicInteger());
        nodes_after.forEach((node,count)->count.set(0));

        node2keys_init.values()
                .stream()
                .flatMap(keys->keys.stream())
                .forEach(key->{
                    String node = consistentHash.calculateNode(key);
                    Assert.assertTrue("algo returned non existing node", nodes_after.containsKey(node));
                    node2keys_after.get(node).add(key);
                    nodes_after.get(node).incrementAndGet();
                });



        nodes_after.forEach((node,count)-> {
            System.out.println("node: " + node + " count: " + count.get() + " key size: " + node2keys_after.get(node).size());

        });

        sum = nodes_after.values()
                .stream()
                .reduce((l, r) -> new AtomicInteger(l.get() + r.get())).get().get();
        System.out.println("sum: " + sum);


        AtomicLong movedKeysCount = new AtomicLong(0);
        /**
         * Phase 3
         * Validate re-balancing was as expected
         */
         node2keys_init.forEach((node,keys)->{
             long movedCount =
                keys.stream()
                    .filter(key -> !match(node2keys_after, node, key))
                    .count();
             movedKeysCount.addAndGet(movedCount);
        });
        System.out.println("moved (rebalanced) keys count: " + movedKeysCount.get());


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
