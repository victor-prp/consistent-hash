package victor.prp.consistent.hash;

import org.junit.Assert;
import org.junit.Test;

import java.util.Map;
import java.util.Set;

import static victor.prp.consistent.hash.ConsistentHashTestUtil.*;

/**
 * @author victorp
 */
public class ConsistentInputOutputTest {
    private static final int MIN_EXPECTED_COLLISIONS = 50;

    private static final int NODES_COUNT = 100;
    private static final int BUCKETS_COUNT = 10000;
    private static final int KEYS_COUNT = 1000000;



    @Test
    public void validateHashCollisionsDoesNotCreateInconsistency(){

        final Set<String> keys = createRandomKeys(KEYS_COUNT);

        /**
         * Phase 1
         * Simulate with random nodes and random keys
         */
        final Set<String> nodesPhase1 = initNodesRandomly(NODES_COUNT);
        ConsistentHash consistentHashPhase1 = initConsistentHash(BUCKETS_COUNT,nodesPhase1,MIN_EXPECTED_COLLISIONS);
        Map<String,Set<String>> nodes2KeysPhase1 = simulate(consistentHashPhase1, nodesPhase1, keys);

        /**
         * Phase 2
         * Simulate with same nodes from phase 1  and with same keys from phase 1
         */
        final Set<String> nodesPhase2 = initNodes(nodesPhase1);
        ConsistentHash consistentHashPhase2 = initConsistentHash(BUCKETS_COUNT,nodesPhase2,MIN_EXPECTED_COLLISIONS);
        Map<String,Set<String>> nodes2KeysPhase2 = simulate(consistentHashPhase2, nodesPhase2, keys);

        /**
         * Phase 3
         * Validate both phases created exactly same output
         */
        long movedKeysCountResult = calculatedMovedKeys(nodes2KeysPhase1, nodes2KeysPhase2);
        System.out.println("moved (rebalanced) keys count: " + movedKeysCountResult);
        Assert.assertEquals("We expect the simulation in phase 1 produces the same output as in phase 2 ",0,movedKeysCountResult);

    }



    public  static ConsistentHash initConsistentHash(int bucketCount, Set<String> nodes,int minExpectedCollisions){
        ConsistentHash consistentHash = new ConsistentHash(bucketCount);
        int collisionCount = addNodesRandomly(consistentHash, nodes);
        System.out.println("ConsistentHash was created with " + nodes.size()+ " nodes. Collisions during creation: " + collisionCount);
        Assert.assertTrue("Minimum expected collisions is " + minExpectedCollisions, collisionCount > minExpectedCollisions );
        return consistentHash;
    }
}
