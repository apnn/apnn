package SimilarityFile;

import io.github.htools.lib.Log;
import java.util.Comparator;

/**
 * Compares based on the estimated similarity using the similarity measure (e.g. cosine)
 */
public class MeasureSimilarity implements Comparator<SimilarityWritable> {
    public static Log log = new Log(MeasureSimilarity.class);
    public static MeasureSimilarity singleton = new MeasureSimilarity();
    
    @Override
    public int compare(SimilarityWritable o1, SimilarityWritable o2) {
        return Double.compare(o1.measureSimilarity, o2.measureSimilarity);
    }

}
