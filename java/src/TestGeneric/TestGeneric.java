package TestGeneric;

import SimilarityFile.SimilarityFile;
import SimilarityFile.SimilarityWritable;
import SimilarityFunction.CosineSimilarityTFIDF;
import TestGeneric.Candidate;
import TestGenericMR.DocumentReader;
import TestGenericMR.DocumentReaderTerms;
import io.github.htools.io.Datafile;
import io.github.htools.io.HPath;
import io.github.htools.lib.Log;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;

/**
 * A general purpose test framework that shows how to read the archive files for
 * source and suspicious documents in the collection. Note that this non-Hadoop
 * implementation requires the index to either fit in memory or to be stored on
 * disk.
 *
 * @author jeroen
 */
public abstract class TestGeneric {

    public static Log log = new Log(TestGeneric.class);
    DocumentReader reader;
    private SimilarityFile similarityFile;
    protected AnnIndex index;
    private int K = 10;

    public TestGeneric(Comparator<SimilarityWritable> comparator) throws IOException, ClassNotFoundException {
        index = getIndex(comparator);
        reader = getDocumentReader();
    }

    public TestGeneric(Datafile vocabulary, Comparator<SimilarityWritable> comparator) throws IOException, ClassNotFoundException {
        Document.setSimilarityFunction(new CosineSimilarityTFIDF(vocabulary));
        index = getIndex(comparator);
        reader = getDocumentReader();
    }
    
    public DocumentReader getDocumentReader() {
        return new DocumentReaderTerms();
    }

    protected void setupOutput(Datafile outputFile) throws IOException {
        similarityFile = new SimilarityFile(outputFile);
        similarityFile.openWrite();
    }

    protected void closeOutput() throws IOException {
        similarityFile.closeWrite();
    }

    /**
     * Writes the top-k most similar source documents for a given suspicious
     * document to the SimilarityFile.
     *
     * @param queryDocument
     * @param topKSourceDocuments
     * @throws IOException
     */
    protected void writeSimilarities(Document queryDocument,
            CandidateList topKSourceDocuments) throws IOException {
        for (Candidate candidate : topKSourceDocuments.sorted()) {
            candidate.id = queryDocument.docid;
            candidate.write(similarityFile);
        }
    }

    /**
     * Load all documents from the ArchiveFile in the given sourcePath into the
     * index by calling add(Document) on the index.
     *
     * @param sourcePath
     * @throws IOException
     */
    public void loadSourceDocuments(HPath sourcePath) throws IOException {
        ArrayList<Datafile> files = sourcePath.getFiles();
        while (files.size() > 0) {
            Datafile file = files.remove(0);
            log.info("%s", file.getCanonicalPath());
            for (Document d : reader.iterableDocuments(file)) {
                index.add(d);
            }
        }
        index.finishIndex();
    }

    /**
     * Loads the query documents (suspicious documents in PAN11) one-at-a-time,
     * and retrieves the k-most similar source documents from the index. Calls
     * processTopKNN to process the results (e.g. write to file).
     *
     * @param queryPath
     * @throws IOException
     */
    public void streamQueryDocuments(HPath queryPath) throws IOException {
        for (Datafile file : queryPath.getFiles()) {
            log.info("%s", file.getCanonicalPath());
            for (Document d : reader.iterableDocuments(file)) {
                log.info("Document %s", d.docid);
                CandidateList topknn = index.getNNs(d, K);
                processTopKNN(d, topknn);
            }
        }
    }

    /**
     * This method is called for every suspicousDocument with a map of the 
     * k-most similar source documents retrieved from the index. Implementations
     * of this method should process the results, e.g. write the results to file.
     * @param suspiciousDocument
     * @param topk
     * @throws IOException 
     */
    public abstract void processTopKNN(Document suspiciousDocument, 
            CandidateList topk) throws IOException;

    /**
     * @return an instance of the AnnIndex class used.
     * @throws ClassNotFoundException 
     */
    public abstract AnnIndex getIndex(Comparator<SimilarityWritable> comparator) throws ClassNotFoundException;
}
