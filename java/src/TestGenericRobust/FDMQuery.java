package TestGenericRobust;

import io.github.htools.lib.Log;
import java.util.ArrayList;
import java.util.HashSet;

/**
 * @author Jeroen
 */
public class FDMQuery {
    public static Log log = new Log(FDMQuery.class);
    public String id;
    public ArrayList<String> termlist;
    public HashSet<String> termset;
    public ArrayList<String> uniqtermlist;

    public FDMQuery(String id, ArrayList<String> terms) {
        log.info("query %s %s", id, terms);
        this.id = id;
        this.termlist = terms;
        this.termset = new HashSet(terms);
        this.uniqtermlist = new ArrayList(termset);
    }
}
