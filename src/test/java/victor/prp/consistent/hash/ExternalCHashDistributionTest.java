package victor.prp.consistent.hash;


import java.util.*;
import com.sun.jersey.api.client.Client;
import org.junit.Test;

/**
 *
 * This test is not designed to run automatically <br>
 * The main purpose is to test distribution of a consistent hashing algorithm in real life system <br>
 * The system should be configured with a router (that forward the requests according to consistent hashing algo) <br>
 * and backend servers the simply respond with 200 OK and some static content (which uniquely identifies this backend node) <br>
 * The output is simply a number represents the deviation (percentage) of the requests distribution from the average <br>
 *
 * @author victorp
 */
public class ExternalCHashDistributionTest {

    //Each request will be sent to url staring form this prefix, ending with random generated unique id
    //If you set nginx as the router corresponding upstream should be defined
    private static final String ROUTER_URL_PREFIX =  "http://localhost/api/test?key=";


    private static final String ERROR = "_error";
    private static final int TOTAL_REQUESTS = 1000000;


    //Uncomment and run the test
    //@Test

    /**
     * The test assumes each backend always responds with the same content which is unique (different from all other backhands) <br>
     * Sequentially runs all requests (without any delay between each) <br>
     * Calculates and prints the deviation from the average <br>
     */
    public void runAndPrintDeviationFromAvg(){

        Map<String,Integer> distributionMap = new HashMap<>();
        Client client = new Client();
        client.setReadTimeout(20000);
        client.setConnectTimeout(20000);

        for (int i =0; i< TOTAL_REQUESTS; i++){
            String uid = UUID.randomUUID().toString();
            String getResponse = doGet(client, uid);
            updateDistributionMap(distributionMap, getResponse);
            System.out.println("req seq: "+i + ", res: "+getResponse);
        }

        Integer errorCount = distributionMap.remove(ERROR);
        int totalErrors = errorCount != null ? errorCount : 0;
        System.out.println("Total errors "+totalErrors);

        List<Integer> sortedDistribution = new LinkedList<>(distributionMap.values());
        Collections.sort(sortedDistribution);

        printDistribution(sortedDistribution);
        System.out.println("Distribution Dis-balance (deviation from the average in percentage): "+calculateDistributionDisbalance(sortedDistribution,TOTAL_REQUESTS));


    }

    private int calculateDistributionDisbalance(List<Integer> sortedDistribution,int totalRequests) {
        int expectedCountPerNode = totalRequests / sortedDistribution.size();
        int first = sortedDistribution.get(1); //the very first is for errors (n/a)
        int last = sortedDistribution.get(sortedDistribution.size()-1);

        int deviation = Math.max(expectedCountPerNode -first,last - expectedCountPerNode);
        return (100*deviation)/expectedCountPerNode;
    }

    private void printDistribution(List<Integer> sortedDistribution) {
        System.out.print("Distribution: ");
        for (int count : sortedDistribution){
            System.out.print(count + ", ");
        }
        System.out.println();
    }

    private void updateDistributionMap(Map<String, Integer> distributionMap, String getResponse) {
        Integer oldCount = distributionMap.get(getResponse);
        int newCount = 1;
        if (oldCount != null){
            newCount = oldCount +1;
        }
        distributionMap.put(getResponse,newCount);
    }

    private String doGet(Client client, String uid) {

        String result = ERROR;
        try {
            result = client.resource(ROUTER_URL_PREFIX + uid).get(String.class);
        }catch (RuntimeException e){
            System.out.print("http request failed: " + e.getMessage());
        }
        return result;
    }

}

