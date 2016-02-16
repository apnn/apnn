package Canopy;

import SimilarityFile.SimilarityWritable;
import TestGeneric.Document;
import io.github.htools.collection.TopKMap;
import io.github.htools.lib.Log;
import io.github.htools.type.TermVectorDouble;
import org.apache.hadoop.conf.Configuration;

import java.util.Comparator;
import java.util.HashSet;
import java.util.Set;

/**
 * An ANN that uses a cheap similarity function to assign documents to canopies
 * and within each canopy the true similarity function to evaluate the goodness
 * of fit; a document that is not within the threshold of the centroid of any
 * canopy according to the true distance will seed a new canopy.
 *
 * @author Jeroen
 */
public class AnnCanopyCosine extends AnnCanopy<Set<String>> {

    public static Log log = new Log(AnnCanopyCosine.class);
    int threshold = 1;

    public AnnCanopyCosine(
            Comparator<SimilarityWritable> comparator, double t1, double t2, int k) {
        super(comparator, t1, t2, k);
        threshold = 1;
    }

    public AnnCanopyCosine(Comparator<SimilarityWritable> comparator, Configuration conf) {
        super(comparator, conf);
        threshold = 1;
    }

    @Override
    protected Set<String> getFingerprintSource(Document document) {
        // returns a 'shortVector' of the top-k n-tfidf terms
        // take top-k tfidf terms
        double maxtfidf = Double.MIN_VALUE;
        String max = null;
        if (document.getModel().size() < k) {
            return ((TermVectorDouble) document.getModel()).keySet();
        }
        TopKMap<Double, String> topk = new TopKMap(k, ((TermVectorDouble) document.getModel()).invert());
        return new HashSet(topk.values());
    }


        public double fastDistance(Doc<Set<String>> a, Doc<Set<String>> b) {
            int count = 0;
            for (String terma : a.getKey()) {
                if (b.key.contains(terma)) {
                    count++;
                }
            }
            return count==0?1:(0.5 - 0.5 * count / (double) this.k);
        }

        public double distance(Doc<Set<String>> a, Doc<Set<String>> b) {
            return 1 - a.getModel().cossim(b.getModel());
        }
}
