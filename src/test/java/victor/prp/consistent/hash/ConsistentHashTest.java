package victor.prp.consistent.hash;

import static victor.prp.consistent.hash.ConsistentHashTestUtil.*;
import org.junit.Assert;
import org.junit.Test;

import java.util.*;

/**
 * @author victorp
 */
public class ConsistentHashTest {
    private static final int NODES_COUNT = 10;
    private static final int BUCKETS_COUNT =  30000;
    private static final int KEYS_COUNT = 1000000;

    @Test
    public void validateDistribution(){
        double expectedDeviation = 0.05; //5%

        final Set<String> keys = createRandomKeys(KEYS_COUNT);
        final Set<String> nodes = initNodesRandomly(NODES_COUNT);

        Map<String,Set<String>> node2Keys = simulate(BUCKETS_COUNT,nodes,keys);

        int expectedCountPerNode = KEYS_COUNT / NODES_COUNT;
        node2Keys.forEach((node, keysSet) -> {
            Assert.assertFalse("Count is too big in node: " + node, (double) keysSet.size() > ((double) expectedCountPerNode) * (1.0 + expectedDeviation));
            Assert.assertFalse("Count is too small in node: " + node, (double) keysSet.size() < ((double) expectedCountPerNode) * (1.0 - expectedDeviation));

        });

    }


    @Test
    public void validateRebalancingWhenNodeIsAdded(){

        final Set<String> keys = createRandomKeys(KEYS_COUNT);

        /**
         * Phase 1
         * Simulate with random nodes and random keys
         */
        final Set<String> nodesPhase1 = initNodesRandomly(NODES_COUNT);
        Map<String,Set<String>> nodes2KeysPhase1 = simulate(BUCKETS_COUNT, nodesPhase1, keys);

        /**
         * Phase 2
         * Simulate with same nodes from phase 1 + "new-node" and with same keys from phase 1
         */
        final Set<String> nodesPhase2 = initNodes(nodesPhase1,"new-node");
        Map<String,Set<String>> nodes2KeysPhase2 = simulate(BUCKETS_COUNT, nodesPhase2, keys);

        /**
         * Phase 3
         * Validate re-balancing was as expected
         */
        long movedKeysCountResult = calculatedMovedKeys(nodes2KeysPhase1, nodes2KeysPhase2);
        System.out.println("moved (rebalanced) keys count: " + movedKeysCountResult);
        Assert.assertEquals("moved keys count must be equal to keys count assign to 'new-node' at phase 2",nodes2KeysPhase2.get("new-node").size(),movedKeysCountResult);

    }


    @Test
    public void validateRebalancingWhenNodeIsRemoved(){

        final Set<String> keys = createRandomKeys(KEYS_COUNT);
        final Set<String> nodes = initNodesRandomly(NODES_COUNT);

        /**
         * Phase 1
         * Simulate with random nodes new-node" and random keys
         */
        final Set<String> nodesPhase1 = initNodes(nodes, "new-node");
        Map<String,Set<String>> nodes2KeysPhase1 = simulate(BUCKETS_COUNT, nodesPhase1, keys);

        /**
         * Phase 2
         * Simulate with same nodes from phase 1 - "new-node" and with same keys from phase 1
         */
        final Set<String> nodesPhase2 = initNodes(nodes);
        Map<String,Set<String>> nodes2KeysPhase2 = simulate(BUCKETS_COUNT, nodesPhase2, keys);

        /**
         * Phase 3
         * Validate re-balancing was as expected
         */
        long movedKeysCountResult = calculatedMovedKeys(nodes2KeysPhase1, nodes2KeysPhase2);
        System.out.println("moved (rebalanced) keys count: " + movedKeysCountResult);
        Assert.assertEquals("moved keys count must be equal to keys count assign to 'new-node' at phase 1",nodes2KeysPhase1.get("new-node").size(),movedKeysCountResult);

    }

}
