package victor.prp.consistent.hash;

import static victor.prp.consistent.hash.ConsistentHashTestUtil.*;
import org.junit.Assert;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author victorp
 */
public class ConsistentHashTest {
    private static final int NODES_COUNT = 10;
    private static final int BUCKETS_COUNT =  10000;
    private static final int KEYS_COUNT = 1000000;

    @Test
    public void validateDistribution(){
        double expectedDeviation = 0.05; //5%

        final Set<String> keys = createRandomKeys(KEYS_COUNT);
        final Map<String,AtomicInteger> nodes = initNodesRandomly(NODES_COUNT);

        simulate(BUCKETS_COUNT,nodes,keys);

        int expectedCountPerNode = KEYS_COUNT / NODES_COUNT;
        nodes.forEach((node,count)-> {
            Assert.assertFalse("Count is too big in node: " + node, (double) count.get() > ((double) expectedCountPerNode) * (1.0 + expectedDeviation));
            Assert.assertFalse("Count is too small in node: "+node,(double)count.get() < ((double)expectedCountPerNode)*(1.0 - expectedDeviation));

        });

    }


    @Test
    public void validateRebalancingWhenNodeIsAdded(){

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
        Assert.assertEquals("moved keys count must be equal to keys count assign to 'new-node' at phase 2",nodesPhase2.get("new-node").get(),movedKeysCountResult);

    }


    @Test
    public void validateRebalancingWhenNodeIsRemoved(){

        final Set<String> keys = createRandomKeys(KEYS_COUNT);
        final Map<String,AtomicInteger> nodes = initNodesRandomly(NODES_COUNT);

        /**
         * Phase 1
         * Simulate with random nodes new-node" and random keys
         */
        final Map<String,AtomicInteger> nodesPhase1 = initNodes(nodes.keySet(), "new-node");
        Map<String,Set<String>> nodes2KeysPhase1 = simulate(BUCKETS_COUNT, nodesPhase1, keys);

        /**
         * Phase 2
         * Simulate with same nodes from phase 1 - "new-node" and with same keys from phase 1
         */
        final Map<String,AtomicInteger> nodesPhase2 = initNodes(nodes.keySet());
        Map<String,Set<String>> nodes2KeysPhase2 = simulate(BUCKETS_COUNT, nodesPhase2, keys);

        /**
         * Phase 3
         * Validate re-balancing was as expected
         */
        long movedKeysCountResult = calculatedMovedKeys(nodes2KeysPhase1, nodes2KeysPhase2);
        System.out.println("moved (rebalanced) keys count: " + movedKeysCountResult);
        Assert.assertEquals("moved keys count must be equal to keys count assign to 'new-node' at phase 1",nodesPhase1.get("new-node").get(),movedKeysCountResult);

    }

}
