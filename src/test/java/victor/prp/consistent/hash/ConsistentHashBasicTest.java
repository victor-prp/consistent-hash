package victor.prp.consistent.hash;

import static victor.prp.consistent.hash.ConsistentHashTestUtil.*;
import org.junit.Assert;
import org.junit.Test;

import java.util.*;
import java.util.stream.Collectors;

/**
 * @author victorp
 */
public class ConsistentHashBasicTest {
    private static final int NODES_COUNT = 100;
    private static final int BUCKETS_COUNT =  10000;
    private static final int KEYS_COUNT = 1000000;

    @Test
    public void validateDistributionByDefinition(){
        final Set<String> nodes = initNodesRandomly(NODES_COUNT);
        ConsistentHash consistentHash = initConsistentHash(BUCKETS_COUNT, nodes);
        Map<String,Long> distribution =   consistentHash.distribution();

        List<Double> deviations = deviations(distribution.values());
        System.out.println("sorted deviations: "+deviations);

    }

    private List<Double> deviations(Collection<Long> distribution) {
        long sum = distribution.stream().reduce(0L,(prev,curr)-> prev+curr);
        long avg = sum/NODES_COUNT;
        List<Long> sorted = distribution.stream().sorted().collect(Collectors.toList());

        return sorted.stream()
                .map(weight -> deviation(weight,avg))
                .sorted()
                .collect(Collectors.toList());
    }

    private double deviation(long weight, long avgWeight){
        long absDeviation = Math.abs(weight - avgWeight);
        double deviation = ((double)absDeviation/(double)avgWeight)*100;
        return deviation;
    }


    @Test
    public void validateDistribution(){
        double expectedDeviation = 0.05; //5%

        final Set<String> keys = createRandomKeys(KEYS_COUNT);
        final Set<String> nodes = initNodesRandomly(NODES_COUNT);
        ConsistentHash consistentHash = initConsistentHash(BUCKETS_COUNT, nodes);


        List<Double> deviations = deviations(consistentHash.distribution().values());
        System.out.println("Defined deviations: "+deviations);

        Map<String,Set<String>> node2Keys = simulate(consistentHash, nodes,keys);

        List<Long> actualDistribution = node2Keys.values().stream()
                .map(k -> (long)k.size())
                .collect(Collectors.toList());
        List<Double> actualDeviations = deviations(actualDistribution);
        System.out.println("Actual deviations: "+actualDeviations);


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
        ConsistentHash consistentHashPhase1 = initConsistentHash(BUCKETS_COUNT,nodesPhase1);
        Map<String,Set<String>> nodes2KeysPhase1 = simulate(consistentHashPhase1, nodesPhase1, keys);

        /**
         * Phase 2
         * Simulate with same nodes from phase 1 + "new-node" and with same keys from phase 1
         */
        final Set<String> nodesPhase2 = initNodes(nodesPhase1,"new-node");
        ConsistentHash consistentHashPhase2 = initConsistentHash(BUCKETS_COUNT,nodesPhase2);
        Map<String,Set<String>> nodes2KeysPhase2 = simulate(consistentHashPhase2, nodesPhase2, keys);

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
        ConsistentHash consistentHashPhase1 = initConsistentHash(BUCKETS_COUNT,nodesPhase1);
        Map<String,Set<String>> nodes2KeysPhase1 = simulate(consistentHashPhase1, nodesPhase1, keys);

        /**
         * Phase 2
         * Simulate with same nodes from phase 1 - "new-node" and with same keys from phase 1
         */
        final Set<String> nodesPhase2 = initNodes(nodes);
        ConsistentHash consistentHashPhase2 = initConsistentHash(BUCKETS_COUNT,nodesPhase2);

        Map<String,Set<String>> nodes2KeysPhase2 = simulate(consistentHashPhase2, nodesPhase2, keys);

        /**
         * Phase 3
         * Validate re-balancing was as expected
         */
        long movedKeysCountResult = calculatedMovedKeys(nodes2KeysPhase1, nodes2KeysPhase2);
        System.out.println("moved (rebalanced) keys count: " + movedKeysCountResult);
        Assert.assertEquals("moved keys count must be equal to keys count assign to 'new-node' at phase 1",nodes2KeysPhase1.get("new-node").size(),movedKeysCountResult);

    }

}
