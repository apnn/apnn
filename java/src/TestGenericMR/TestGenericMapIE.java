package TestGenericMR;

import SimilarityFile.IndexSimilarity;
import SimilarityFile.SimilarityWritable;
import io.github.htools.lib.Log;
import java.util.Comparator;

/**
 * Every map receives one Value as input, that contains a unique combination of
 * a filename with a block of suspicious documents and a filename with a block
 * of source documents. The default operation is to do a brute force comparison
 * by reading all source documents into memory, and
 * then the suspicious documents are inspected one-at-a-time for matches in the
 * index. The k-most similar source documents are send to the reducer. This
 * generic mapper can be configured by setting the index, similarity function
 * and k in the job, or overridden to add functionality.
 *
 * @author jeroen
 */
public class TestGenericMapIE extends TestGenericMap {

    public static final Log log = new Log(TestGenericMapIE.class);

    @Override
    public Comparator<SimilarityWritable> getComparator() {
        return IndexSimilarity.singleton;
    } 
}
