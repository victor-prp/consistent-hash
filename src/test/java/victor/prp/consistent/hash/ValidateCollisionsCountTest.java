package victor.prp.consistent.hash;

import org.junit.Assert;
import org.junit.Test;

import java.util.Map;
import java.util.Set;

import static victor.prp.consistent.hash.ConsistentHashTestUtil.*;
import static victor.prp.consistent.hash.ConsistentHashTestUtil.calculatedMovedKeys;
import static victor.prp.consistent.hash.ConsistentHashTestUtil.simulate;

/**
 * @author victorp
 */
public class ValidateCollisionsCountTest {
    private static final int MAX_EXPECTED_COLLISIONS = 200;

    private static final int NODES_COUNT = 100;
    private static final int BUCKETS_COUNT = 10000;
    private static final int KEYS_COUNT = 1000000;

    /**
     * @return collisions count during ConsistentHash creation
     */
    public static int initConsistentHash(int bucketCount, Set<String> nodes) {
        ConsistentHash consistentHash = new ConsistentHash(bucketCount);
        int collisionCount = addNodesRandomly(consistentHash, nodes);
        System.out.println("ConsistentHash was created with " + nodes.size() + " nodes. Collisions during creation: " + collisionCount);
        return collisionCount;
    }

    @Test
    public void validateHashCollisionsCountIsNotTooBig() {

        final Set<String> keys = createRandomKeys(KEYS_COUNT);

        /**
         * Simulate with nodes generated based on sequence and random keys
         * nodes: node-1, node-2 etc.
         */
        final Set<String> nodesPhase = initNodesSequentially(NODES_COUNT);
        int collisionCount = initConsistentHash(BUCKETS_COUNT, nodesPhase);
        Assert.assertTrue("Minimum expected collisions is " + MAX_EXPECTED_COLLISIONS, collisionCount <= MAX_EXPECTED_COLLISIONS);

    }
}
