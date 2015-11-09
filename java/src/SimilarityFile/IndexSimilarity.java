package SimilarityFile;

import java.util.Comparator;

/**
 * Compares based on the estimated similarity using the index
 */
public class IndexSimilarity implements Comparator<SimilarityWritable> {
    public static IndexSimilarity singleton = new IndexSimilarity();
    
    @Override
    public int compare(SimilarityWritable o1, SimilarityWritable o2) {
        return Double.compare(o1.indexSimilarity, o2.indexSimilarity);
    }
}
