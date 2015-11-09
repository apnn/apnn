package TestGeneric;

import SimilarityFile.SimilarityWritable;
import io.github.htools.collection.TopK;
import io.github.htools.lib.Log;
import java.util.Comparator;
import java.util.HashMap;

/**
 *
 * @author Jeroen
 */
public class CandidateList extends TopK<Candidate> {
    public static Log log = new Log(CandidateList.class);
    public HashMap<Integer, Candidate> candidateLookup = new HashMap();

    public CandidateList(int k, Comparator<SimilarityWritable> comparator) {
        super(k, comparator);
    }

    @Override
    public Candidate poll() {
        Candidate removed = super.poll();
        candidateLookup.remove(removed.document.docid);
        return removed;
    }
    
    public Candidate add(Document d, double indexSimilarity) {
        Candidate c = new Candidate(d, indexSimilarity); 
        if (add(c)) {
            candidateLookup.put(d.docid, c);
            return c;
        }
        return null;
    }
    
    
}
