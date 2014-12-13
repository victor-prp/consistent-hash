package victor.prp.consistent.hash;

import java.util.Arrays;
import java.util.StringJoiner;

/**
 * @author victorp
 */
public class FileNameBuilder {
    private StringJoiner stringJoiner = new StringJoiner(System.getProperty("file.separator"));

    public FileNameBuilder(String... path) {
        Arrays.stream(path).forEach(stringJoiner::add);
    }

    /**
     * 'fs' is an abbreviation of 'File Separator'
     */
    public FileNameBuilder fs(String next){
        stringJoiner.add(next);
        return this;
    }

    public String toString(){
        return stringJoiner.toString();
    }
}
