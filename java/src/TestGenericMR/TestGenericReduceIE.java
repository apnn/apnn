package TestGenericMR;

import SimilarityFile.IndexSimilarity;
import SimilarityFile.MeasureSimilarity;
import SimilarityFile.SimilarityWritable;
import TestGeneric.Candidate;
import io.github.htools.lib.Log;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

/**
 * reduces all scored similarities between suspicious documents (=key) and all
 * source documents, keeping only the k-most similar source documents per
 * suspicious document.
 *
 * @author jeroen
 */
public class TestGenericReduceIE extends TestGenericReduce {

    public static final Log log = new Log(TestGenericReduceIE.class);
    int scantopk;

    public void setup(Context context) throws IOException {
        super.setup(context);
        scantopk = TestGenericJobIE.getScanTopK(conf);
    }
    
    public Comparator<SimilarityWritable> getComparator() {
        return IndexSimilarity.singleton;
    }
    
    @Override
    public ArrayList<Candidate> finalizeList(ArrayList<Candidate> list, int resultSize) {
        ArrayList<Candidate> result = new ArrayList();
        if (list.size() <= resultSize) {
            return list;
        } else {
            double value = list.get(resultSize - 1).indexSimilarity;
            for (int i = 0; i < resultSize ; i++) {
                result.add(list.get(i));
            }
            for (int i = resultSize; i < list.size() && list.get(i).indexSimilarity == value; i++) {
                result.add(list.get(i));
            }
        }
        return result;
    }
    

}
