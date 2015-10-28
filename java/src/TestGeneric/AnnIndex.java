package TestGeneric;

import SimilarityFunction.SimilarityFunction;
import io.github.htools.collection.TopKMap;
import io.github.htools.lib.ArrayTools;
import io.github.htools.lib.Log;
import java.util.HashSet;

/**
 * An index that can store documents in such a form that for a given query
 * document the most similar other document are found with high probability.
 * Implementations of this class can either construct the index in-memory or
 * store the index to disc. They should implement the addDocument() method to
 * add a document to the index and the getDocuments() method to retrieve a list
 * of most similar documents from the indexed collection. Optionally a
 * getFingerprint() of a generic type T can be used to allow to separately
 * extraction a fingerprint for a document, and then add the document or
 * retrieve documents using that fingerprint. Instead, the getFingerprint()
 * method may return null when fingerprints are not used and getDocuments() can
 * directly operate on the document instead. Fingerprints are hidden from the
 * public interface (externally Documents are added and retrieved).
 *
 * @author jeroen
 * @param <T> Generic type for the fingerprint used
 */
public abstract class AnnIndex<T> {

    public static Log log = new Log(AnnIndex.class);
    SimilarityFunction similarityFunction;

    public AnnIndex(SimilarityFunction similarityFunction) throws ClassNotFoundException {
        this.similarityFunction = similarityFunction;
    }

    /**
     * @param document
     * @return A fingerprint of generic type T for the document, or null when
     * fingerprints are not supported by the index implementation.
     */
    protected abstract T getFingerprint(Document document);

    /**
     * Add a document to the index, to allow retrieving the document for highly
     * similar other documents.
     *
     * @param document
     * @param fingerprint a fingerprint of generic type T, or null when this
     * index does not support fingerprinting.
     */
    protected abstract void addDocument(Document document, T fingerprint);

    /**
     * @param fingerprint a fingerprint for the document, or null when
     * fingerprinting is not supported by the index
     * @param query a document for which the index is scanned to retrieve a list
     * of most similar documents
     * @return a list of candidate nearest neighbor documents for the given
     * query document.
     */
    protected abstract HashSet<Document> getDocuments(T fingerprint, Document query);

    /**
     * Retrieve the (at most) k-most similar documents from the index for the
     * query document and add the query document to the index afterwards.
     *
     * @param query a new document for which the most similar k documents are
     * retrieved and which is then added to the index itself.
     * @param k the maximum number of documents retrieved
     * @return
     */
    public TopKMap<Double, Document> getNNsAndAdd(Document query, int k) {
        T fingerprint = getFingerprint(query);
        TopKMap<Double, Document> topk = getNNs(query, fingerprint, k);
        addDocument(query, fingerprint);
        return topk;
    }

    /**
     * Retrieve a list of at most k-most similar indexed documents for the given
     * query document
     *
     * @param query
     */
    public TopKMap<Double, Document> getNNs(Document query, int k) {
        T fingerprint = getFingerprint(query);
        TopKMap<Double, Document> topk = getNNs(query, fingerprint, k);
        return topk;
    }

    /**
     * Add a document to the index, to allow retrieving the document for highly
     * similar other documents.
     *
     * @param document
     */
    public void add(Document document) {
        T fingerprint = getFingerprint(document);
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
}
