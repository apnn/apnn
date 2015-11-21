package Canopy;

import SimilarityFile.SimilarityWritable;
import TestGeneric.AnnIndex;
import TestGeneric.CandidateList;
import TestGeneric.Document;
import io.github.htools.collection.ArrayMap;
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
public abstract class AnnCanopy<K> extends AnnIndex<K> {

    public static Log log = new Log(AnnCanopy.class);
    ArrayList<Doc> docs = new ArrayList();
    ArrayList<Canopy> canopies = new ArrayList();
    double T1, T2;
    int k = 20;

    public AnnCanopy(
            Comparator<SimilarityWritable> comparator, double t1, double t2, int k) {
        super(comparator);
        this.T1 = t1;
        this.T2 = t2;
        this.k = k;
    }

    public AnnCanopy(Comparator<SimilarityWritable> comparator, Configuration conf) {
        this(comparator, CanopyJob.getT1(conf), CanopyJob.getT2(conf),
             CanopyJob.getTermsSize(conf));
    }

    @Override
    protected void addDocument(Document document, K shortVector) {
        docs.add(new Doc(document, shortVector));
    }

    public void finishIndex() {
        T2 = T1;
        log.info("T1 %f", T1);
        createCanopies();
    }

    public void createCanopies() {
        for (int i = docs.size() - 1; i >= 0; i--) {
            Doc doc = docs.get(i);
            for (Canopy c : canopies) {
                double d = fastDistance(c.centroid, doc);
                if (d <= T1) {
                    c.add(doc);
                    if (distance(c.centroid, doc) <= T2) {
                        doc.bound = true;
                    }
                }
                if (doc.document.docid.equals("9168")) {
                    log.info("%s %d %f %b", doc.document.docid, c.id, d, doc.bound);
                }
            }
            if (!doc.bound) {
                Canopy c = new Canopy(doc);
                canopies.add(c);
                for (int j = docs.size() - 1; j > i; j--) {
                    Doc docb = docs.get(j);
                    double d = fastDistance(c.centroid, docb);
                    if (d < T1) {
                        c.add(docb);
                    }
                }
            }
        }
        log.info("%d %d", docs.size(), canopies.size());
        docs = null;
    }

    public double getMaxNN() {
        double maxnn = Double.MIN_VALUE;
        for (int i = 1; i < docs.size(); i++) {
            Doc a = docs.get(i);
            double mindist = Double.MAX_VALUE;
            for (int j = 0; j < i; j++) {
                Doc b = docs.get(j);
                mindist = Math.min(mindist, fastDistance(a, b));
            }
            log.info("mindist %f", mindist);
            maxnn = Math.max(maxnn, mindist);
        }
        return maxnn;
    }

    @Override
    protected void getDocuments(CandidateList candidates,
            K shortVector, Document document) {
        Doc doc = new Doc(document, shortVector);
        HashSet<String> alreadyadded = new HashSet();

        for (Canopy c : canopies) {
            if (fastDistance(c.centroid, doc) < T1) {
                log.info("addDocuments %s Canopy %d %d", doc.document.docid, c.id, c.members.size());
                for (Doc d : c.members) {
                    if (!alreadyadded.contains(d.document.docid) && fastDistance(doc, d) < T1) {
                        double distance = distance(d, doc);
                        log.info("addDocuments %s %s %f %f", doc.document.docid, d.document.docid,
                                fastDistance(d, doc), distance);
                        candidates.add(d.document, 1 - distance);
                        alreadyadded.add(d.document.docid);
                    }
                }
            }
        }
    }

    protected abstract double fastDistance(Doc<K> a, Doc<K> b);

    protected abstract double distance(Doc<K> a, Doc<K> b);

    static int iid = 0;

    public class Canopy {

        int id = iid++;
        ArrayList<Doc> members = new ArrayList();
        Doc centroid;

        public Canopy(Doc centroid) {
            this.centroid = centroid;
            members.add(centroid);
        }

        public void add(Doc doc) {
            members.add(doc);
        }
    }

    public class Doc<K> {

        Document document;
        K key;
        boolean bound = false;

        public Doc(Document document, K key) {
            this.document = document;
            this.key = key;
        }

        public K getKey() {
            return key;
        }

        public TermVectorDouble getModel() {
            return (TermVectorDouble) document.getModel();
        }
    }
}
