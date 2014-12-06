package victor.prp.consistent.hash;

import java.util.function.Function;
import java.util.function.IntFunction;

/**
 * @author victorp
 */
@FunctionalInterface
public interface HashFunction {
    long apply(String key);
}
