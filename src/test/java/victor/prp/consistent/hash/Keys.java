package victor.prp.consistent.hash;

import javax.xml.bind.annotation.XmlType;
import java.util.HashSet;
import java.util.Set;

/**
 * @author victorp
 */
@XmlType
public class Keys {
    private HashSet<String> key = new HashSet<>();

    public Keys() {
    }

    public Keys(Set<String> keys) {
        this.key.addAll(keys);
    }

    public HashSet<String> getKey() {
        return key;
    }

    public void setKey(HashSet<String> key) {
        this.key = key;
    }
}
