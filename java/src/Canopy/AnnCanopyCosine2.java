package Canopy;

import SimilarityFile.SimilarityWritable;
import TestGeneric.Document;
import io.github.htools.lib.Log;
import org.apache.hadoop.conf.Configuration;

import java.util.Comparator;

/**
 * An ANN that uses a cheap similarity function to assign documents to canopies
 * and within each canopy the true similarity function to evaluate the goodness
 * of fit; a document that is not within the threshold of the centroid of any
 * canopy according to the true distance will seed a new canopy.
 *
 * @author Jeroen
 */
public class AnnCanopyCosine2 extends AnnCanopy {

    public static Log log = new Log(AnnCanopyCosine2.class);

    public AnnCanopyCosine2(
            Comparator<SimilarityWritable> comparator, double t1, double t2, int k) {
        super(comparator, t1, t2, k);
    }

    public AnnCanopyCosine2(Comparator<SimilarityWritable> comparator, Configuration conf) {
        super(comparator, conf);
    }

    @Override
    public Object getFingerprintSource(Document document) {
        return null;
    }

    @Override
    public double fastDistance(Doc a, Doc b) {
        return 1 - a.getModel().dotproduct(b.getModel());
    }

    @Override
    public double distance(Doc a, Doc b) {
        return 1 - a.getModel().dotproduct(b.getModel());
    }
}
