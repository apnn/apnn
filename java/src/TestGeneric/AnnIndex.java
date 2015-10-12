package TestGeneric;

import SimilarityFunction.SimilarityFunction;
import TestGenericMR.TestGenericJob;
import io.github.htools.collection.TopKMap;
import io.github.htools.lib.Log;
import java.util.HashSet;
import org.apache.hadoop.conf.Configuration;

/**
 *
 * @author iloen
 */
public abstract class AnnIndex<T> {

    public static Log log = new Log(AnnIndex.class);
    SimilarityFunction similarityFunction;

    public AnnIndex(SimilarityFunction similarityFunction) throws ClassNotFoundException {
        this.similarityFunction = similarityFunction;
    }

    protected abstract T getFingerPrint(Document document);

    protected abstract void addDocument(Document document, T fingerprint);

    protected abstract HashSet<Document> getDocuments(T fingerprint, Document document);

    public TopKMap<Double, Document> getNNsAndAdd(Document document, int k) {
        T fingerprint = getFingerPrint(document);
        TopKMap<Double, Document> topk = getNNs(document, fingerprint, k);
        addDocument(document, fingerprint);
        return topk;
    }

    public TopKMap<Double, Document> getNNs(Document document, int k) {
        T fingerprint = getFingerPrint(document);
        TopKMap<Double, Document> topk = getNNs(document, fingerprint, k);
        return topk;
    }

    public void add(Document document) {
        T fingerprint = getFingerPrint(document);
        addDocument(document, fingerprint);
    }

    private TopKMap<Double, Document> getNNs(Document document, T fingerprint, int k) {
        TopKMap<Double, Document> topk = new TopKMap(k);
        HashSet<Document> documents = getDocuments(fingerprint, document);
        for (Document candidate : documents) {
            double score = similarityFunction.similarity(document, candidate);
            topk.add(score, candidate);
        }
        return topk;
    }
    
    public int getComparisons() {
        return similarityFunction.getComparisons();
    }
}
