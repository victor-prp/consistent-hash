package victor.prp.consistent.hash;

import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author victorp
 */
@XmlRootElement
public class Nodes2Keys {
    private HashMap<String, Keys> node2Keys = new HashMap<>();

    public Nodes2Keys() {
    }


    public Nodes2Keys(Map<String, Set<String>> node2Keys) {
        node2Keys.forEach((node,keys)-> this.node2Keys.put(node,new Keys(keys)));
    }

    public HashMap<String, Keys> getNode2Keys() {
        return node2Keys;
    }

    public void setNode2Keys(HashMap<String, Keys> node2Keys) {
        this.node2Keys = node2Keys;
    }

    Set<String> nodes(){
        return node2Keys.keySet();
    }

    Set<String> allKeys(){
        return
            node2Keys.values().stream()
                .flatMap(keys->keys.getKey().stream())
                .collect(Collectors.toSet());
    }

    public Map<String, Set<String>> getNode2KeysSet() {
        Map<String, Set<String>> result = new HashMap<>();
        node2Keys.forEach((node,keys)->result.put(node,keys.getKey()));
        return result;
    }
}
