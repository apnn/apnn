package Canopy;

import SimilarityFile.SimilarityWritable;
import TestGeneric.CandidateList;
import TestGeneric.Document;
import io.github.htools.lib.Log;
import io.github.htools.type.TermVectorDouble;
import org.apache.hadoop.conf.Configuration;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;

/**
 * An ANN that uses a cheap similarity function to assign documents to canopies
 * and within each canopy the true similarity function to evaluate the goodness
 * of fit; a document that is not within the threshold of the centroid of any
 * canopy according to the true distance will seed a new canopy.
 *
 * @author Jeroen
 */
public abstract class AnnCanopy<K> extends TestGeneric.AnnIndex<K> {

    public static Log log = new Log(AnnCanopy.class);
    ArrayList<Doc<K>> docs = new ArrayList();
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
    protected void addDocument(Document document, K fingerprint) {
        Doc<K> d = new Doc(document, fingerprint);
        docs.add(d);
    }

    public Doc<K> createDocumentSource(Document document) {
        return new Doc(document, this.getFingerprintSource(document));
    }

    public Doc<K> createDocumentQuery(Document document) {
        return new Doc(document, this.getFingerprintQuery(document));
    }

    public void finishIndex() {
        T2 = T1;
        log.info("T1 %f", T1);
        createCanopies();
    }

    public void createCanopies() {
        for (int i = docs.size() - 1; i >= 0; i--) {
            Doc<K> doca = docs.get(i);
            for (Canopy c : canopies) {
                double d = fastDistance(c.centroid, doca);
                if (d <= T1) {
                    c.add(doca);
                    if (distance(c.centroid, doca) <= T2) {
                        doca.bound = true;
                        log.info("%s %d %f %b", doca.document.docid, c.id, d, doca.bound);
                    }
                }
            }
            if (!doca.bound) {
                Canopy c = new Canopy(doca);
                canopies.add(c);
                log.info("new canopy %s %d", doca.document.docid, c.id);
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
            K fingerprint, Document document) {
        HashSet<String> alreadyadded = new HashSet();
        Doc doc = new Doc(document, fingerprint);
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

    public abstract double fastDistance(Doc<K> a, Doc<K> b);

    public abstract double distance(Doc<K> a, Doc<K> b);

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

    public static class Doc<K> {

        Document document;
        K key;
        boolean bound = false;

        public Doc(Document document, K key) {
            this.document = document;
            this.key = key;
        }

        public Document getDocument() {
            return document;
        }
        
        public K getKey() {
            return key;
        }

        public TermVectorDouble getModel() {
            return (TermVectorDouble) document.getModel();
        }
    }
}
