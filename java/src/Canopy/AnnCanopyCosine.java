package Canopy;

import SimilarityFile.SimilarityWritable;
import TestGeneric.AnnIndex;
import TestGeneric.CandidateList;
import TestGeneric.Document;
import io.github.htools.collection.TopKMap;
import io.github.htools.lib.CollectionTools;
import io.github.htools.lib.Log;
import io.github.htools.type.TermVectorDouble;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import org.apache.hadoop.conf.Configuration;

/**
 * An ANN that uses a cheap similarity function to assign documents to canopies
 * and within each canopy the true similarity function to evaluate the goodness
 * of fit; a document that is not within the threshold of the centroid of any
 * canopy according to the true distance will seed a new canopy.
 *
 * @author Jeroen
 */
public class AnnCanopyCosine extends AnnCanopy<TermVectorDouble> {

    public static Log log = new Log(AnnCanopyCosine.class);

    public AnnCanopyCosine(
            Comparator<SimilarityWritable> comparator, double t1, double t2, int k) {
        super(comparator, t1, t2, k);
    }

    public AnnCanopyCosine(Comparator<SimilarityWritable> comparator, Configuration conf) {
        super(comparator, conf);
    }

    @Override
    protected TermVectorDouble getFingerprint(Document document) {
        // returns a 'shortVector' of the top-k n-tfidf terms
        // take top-k tfidf terms
        double maxtfidf = Double.MIN_VALUE;
        String max = null;
        if (document.getModel().size() < k) {
            return (TermVectorDouble) document.getModel();
        }
        TopKMap<Double, String> topk = new TopKMap(k, ((TermVectorDouble) document.getModel()).invert());
        TermVectorDouble model = new TermVectorDouble(k);
        CollectionTools.invert(topk, model);
        return model;
    }


        protected double fastDistance(Doc<TermVectorDouble> a, Doc<TermVectorDouble> b) {
            return 1 - a.getKey().cossim(b.getKey());
        }

        protected double distance(Doc<TermVectorDouble> a, Doc<TermVectorDouble> b) {
            return 1 - a.getModel().cossim(b.getModel());
        }
}
