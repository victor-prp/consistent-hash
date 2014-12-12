package victor.prp.consistent.hash;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * @author victorp
 */
public class CollectionsUtil {

    private CollectionsUtil() {
    }

    public static <T> Set<T> asSet(T... array){
        Set<T> result = new HashSet<>();
        result.addAll(Arrays.asList(array));
        return result;
    }
}
