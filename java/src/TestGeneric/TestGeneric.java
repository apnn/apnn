package TestGeneric;

import SimilarityFile.SimilarityFile;
import SimilarityFile.SimilarityWritable;
import TestGeneric.Candidate;
import io.github.htools.io.Datafile;
import io.github.htools.io.HPath;
import io.github.htools.io.compressed.ArchiveFile;
import io.github.htools.io.compressed.ArchiveEntry;
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
    private SimilarityFile similarityFile;
    protected AnnIndex index;
    private int K = 10;

    public TestGeneric(Comparator<SimilarityWritable> comparator) throws IOException, ClassNotFoundException {
        index = getIndex(comparator);
    }

    public TestGeneric(Datafile vocabulary, Comparator<SimilarityWritable> comparator) throws IOException, ClassNotFoundException {
        index = getIndex(vocabulary, comparator);
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
     * @param suspiciousDocument
     * @param topKSourceDocuments
     * @throws IOException
     */
    protected void writeSimilarities(Document suspiciousDocument,
            CandidateList topKSourceDocuments) throws IOException {
        for (Candidate candidate : topKSourceDocuments.sorted()) {
            candidate.id = suspiciousDocument.docid;
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
            ArchiveFile sourceFile = ArchiveFile.getReader(file);
            for (ArchiveEntry entry : (Iterable<ArchiveEntry>) sourceFile) {
                Document document = Document.read(entry);
                index.getSimilarityFunction().reweight(document);
                //log.info("%d %s", document.docid, document.getTerms());
                index.add(document);
            }
        }
    }

    /**
     * Loads the query documents (suspicious documents in PAN11) one-at-a-time,
     * and retrieves the k-most similar source documents from the index. Calls
     * processTopKNN to process the results (e.g. write to file).
     *
     * @param suspiciousPath
     * @throws IOException
     */
    public void streamSuspiciousDocuments(HPath suspiciousPath) throws IOException {
        for (Datafile file : suspiciousPath.getFiles()) {
            ArchiveFile suspiciousFile = ArchiveFile.getReader(file);
            for (ArchiveEntry entry : (Iterable<ArchiveEntry>) suspiciousFile) {
                Document suspiciousDocument = Document.read(entry);
                index.getSimilarityFunction().reweight(suspiciousDocument);
                CandidateList topknn = index.getNNs(suspiciousDocument, K);
                processTopKNN(suspiciousDocument, topknn);
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
    
    /**
     * @return an instance of the AnnIndex class used.
     * @throws ClassNotFoundException 
     */
    public abstract AnnIndex getIndex(Datafile vocabulary, Comparator<SimilarityWritable> comparator) throws ClassNotFoundException;
}
