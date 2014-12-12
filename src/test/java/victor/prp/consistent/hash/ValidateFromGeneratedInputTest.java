package victor.prp.consistent.hash;

import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.net.URL;
import java.util.Map;
import java.util.Set;

import static victor.prp.consistent.hash.ConsistentHashTestUtil.*;

/**
 * @author victorp
 */
public class ValidateFromGeneratedInputTest {
    private static final int MIN_EXPECTED_COLLISIONS = 30;

    private static final String FILE_NAME = "victor/prp/consistent/hash/node2Keys-00.txt";
    private static final int NODES_COUNT = 100;
    private static final int BUCKETS_COUNT = 10000;
    private static final int KEYS_COUNT = 1000000;

    @Test
    public void validateSameOutput() {

        URL fileUrl = ConsistentInputOutputTest.class.getClassLoader().getResource(FILE_NAME);
        File node2Keys00 = new File(fileUrl.getFile());
        Nodes2Keys nodes2KeysFromFile= JaxbUtil.unmarshal(node2Keys00,Nodes2Keys.class);

        Set<String> nodes = nodes2KeysFromFile.nodes();
        Set<String> keys = nodes2KeysFromFile.allKeys();

        ConsistentHash consistentHash = initConsistentHash(BUCKETS_COUNT, nodes,MIN_EXPECTED_COLLISIONS);

        Map<String, Set<String>> node2KeysFromSimulation = simulate(consistentHash, nodes, keys);

        long movedKeys = calculatedMovedKeys(nodes2KeysFromFile.getNode2KeysSet(), node2KeysFromSimulation);
        Assert.assertEquals("We expect the simulation will produce the same output as in the file", 0, movedKeys);

    }


    /**
     * Run it in order to generate the input according to current algorithm impl
     */
    public static void main(String... args) {
        generateInput();
    }

    private static void generateInput() {
        final Set<String> keys = createRandomKeys(KEYS_COUNT);
        final Set<String> nodes = initNodesRandomly(NODES_COUNT);
        ConsistentHash consistentHash = initConsistentHash(BUCKETS_COUNT, nodes,MIN_EXPECTED_COLLISIONS);
        Map<String, Set<String>> node2Keys = simulate(consistentHash, nodes, keys);
        URL fileUrl = ConsistentInputOutputTest.class.getClassLoader().getResource(FILE_NAME);
        Nodes2Keys nodes2KeysForJaxb = new Nodes2Keys(node2Keys);
        File node2Keys00 = new File(fileUrl.getFile());
        JaxbUtil.marshal(nodes2KeysForJaxb, node2Keys00);
    }

    public  static ConsistentHash initConsistentHash(int bucketCount, Set<String> nodes,int minExpectedCollisions){
        ConsistentHash consistentHash = new ConsistentHash(bucketCount);
        int collisionCount = addNodesRandomly(consistentHash, nodes);
        System.out.println("ConsistentHash was created with " + nodes.size()+ " nodes. Collisions during creation: " + collisionCount);
        Assert.assertTrue("Minimum expected collisions is " + minExpectedCollisions, collisionCount > minExpectedCollisions );
        return consistentHash;
    }



}
