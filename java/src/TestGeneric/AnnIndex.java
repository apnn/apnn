package TestGeneric;

import SimilarityFile.SimilarityWritable;
import SimilarityFunction.SimilarityFunction;
import io.github.htools.lib.Log;
import io.github.htools.lib.Profiler;
import java.io.IOException;
import java.util.Comparator;

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
    private static final Profiler GETDOCUMENTSTIME = Profiler.getProfiler("GETDOCUMENTSTIME");
    private static final Profiler FINGERPRINTTIME = Profiler.getProfiler("FINGERPRINTTIME");
    protected SimilarityFunction similarityFunction;
    protected Comparator<SimilarityWritable> comparator;
    public long countDocCodepoints;
    public long countComparedDocCodepoints;

    public AnnIndex(SimilarityFunction similarityFunction, Comparator<SimilarityWritable> comparator) throws ClassNotFoundException {
        this.similarityFunction = similarityFunction;
        this.comparator = comparator;
    }

    public SimilarityFunction getSimilarityFunction() {
        return similarityFunction;
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
     * @param list
     * @param fingerprint a fingerprint for the document, or null when
     * fingerprinting is not supported by the index
     * @param query a document for which the index is scanned to retrieve a list
     * of most similar documents
     * @return a list of candidate nearest neighbor documents for the given
     * query document.
     */
    protected abstract void getDocuments(CandidateList list, T fingerprint, Document query);

    /**
     * Retrieve the (at most) k-most similar documents from the index for the
     * query document and add the query document to the index afterwards.
     *
     * @param query a new document for which the most similar k documents are
     * retrieved and which is then added to the index itself.
     * @param k the maximum number of documents retrieved
     * @return
     */
    public CandidateList getNNsAndAdd(Document query, int k) throws IOException {
        FINGERPRINTTIME.startTime();
        T fingerprint = getFingerprint(query);
        FINGERPRINTTIME.addAvgTime();
        CandidateList topk = getNNs(query, fingerprint, k);
        addDocument(query, fingerprint);
        return topk;
    }

    /**
     * Retrieve a list of at most k-most similar indexed documents for the given
     * query document
     *
     * @param query
     */
    public CandidateList getNNs(Document query, int k) throws IOException {
        FINGERPRINTTIME.startTime();
        T fingerprint = getFingerprint(query);
        FINGERPRINTTIME.addAvgTime();
        CandidateList topk = getNNs(query, fingerprint, k);
        return topk;
    }

    /**
     * Add a document to the index, to allow retrieving the document for highly
     * similar other documents.
     *
     * @param document
     */
    public void add(Document document) throws IOException {
        FINGERPRINTTIME.startTime();
        T fingerprint = getFingerprint(document);
        FINGERPRINTTIME.addAvgTime();
        addDocument(document, fingerprint);
    }

    public static long getGetDocumentsTime() {
        return GETDOCUMENTSTIME.getTotalTimeMs();
    }
    
    public static long getGetDocumentsCount() {
        return GETDOCUMENTSTIME.getCount();
    }
    
    public static long getGetFingerprintTime() {
        return FINGERPRINTTIME.getTotalTimeMs();
    }
    
    public static long getGetFingerprintCount() {
        return FINGERPRINTTIME.getCount();
    }
    
    private CandidateList getNNs(Document document, T fingerprint, int k) throws IOException {
        CandidateList candidates = new CandidateList(k, comparator);
        GETDOCUMENTSTIME.startTime();
        getDocuments(candidates, fingerprint, document);
        GETDOCUMENTSTIME.addAvgTime();
        assignMeasureSimilarity(candidates, document);
        return candidates;
    }
    
    protected void assignMeasureSimilarity(CandidateList candidates, Document document) {
        for (Candidate candidate : candidates) {
            candidate.measureSimilarity = similarityFunction.similarity(document, candidate.document);
            candidate.id = document.docid;
        }
    }
}
