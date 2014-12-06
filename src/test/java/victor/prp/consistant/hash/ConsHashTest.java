package victor.prp.consistant.hash;

import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

/**
 * @author victorp
 */
public class ConsHashTest {


    @Test
    public void validateRehashingOnAdd(){
        int NODES_COUNT = 100;
        int BUCKETS_COUNT =  10000;
        int KEYS_COUNT = 1000000;

        final Map<String,AtomicInteger> nodes = initNodesRandomly(NODES_COUNT);

        ConsHash consHash = new ConsHash(BUCKETS_COUNT,nodes.keySet());

        IntStream.rangeClosed(1,KEYS_COUNT)
                .forEach(k->{
                    String randomKey  = UUID.randomUUID().toString();
                    String node = consHash.calculateNode(randomKey);
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
    @Test
    public void validateDistribution(){
        int NODES_COUNT = 100;
        int BUCKETS_COUNT =  10000;
        int KEYS_COUNT = 1000000;

        final Map<String,AtomicInteger> nodes = initNodesRandomly(NODES_COUNT);

        ConsHash consHash = new ConsHash(BUCKETS_COUNT,nodes.keySet());

        IntStream.rangeClosed(1,KEYS_COUNT)
                .forEach(k->{
                    String randomKey  = UUID.randomUUID().toString();
                    String node = consHash.calculateNode(randomKey);
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
