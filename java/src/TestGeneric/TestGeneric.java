package TestGeneric;

import SimilarityFile.SimilarityFile;
import SimilarityFile.SimilarityWritable;
import io.github.htools.collection.TopKMap;
import io.github.htools.io.Datafile;
import io.github.htools.io.HPath;
import io.github.htools.io.compressed.ArchiveFile;
import io.github.htools.io.compressed.ArchiveEntry;
import io.github.htools.lib.Log;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;

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
    SimilarityFile similarityFile;
    AnnIndex index;
    int K = 10;

    public TestGeneric() throws IOException, ClassNotFoundException {
        index = getIndex();
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
            TopKMap<Double, Document> topKSourceDocuments) throws IOException {
        SimilarityWritable record = new SimilarityWritable();
        record.id = suspiciousDocument.docid;
        for (Map.Entry<Double, Document> entry : topKSourceDocuments.sorted()) {
            record.source = entry.getValue().docid;
            record.score = entry.getKey();
            record.write(similarityFile);
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
                Document document = new Document(entry);
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
                Document susipiciousDocument = new Document(entry);
                TopKMap<Double, Document> topknn = index.getNNs(susipiciousDocument, K);
                processTopKNN(susipiciousDocument, topknn);
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
            TopKMap<Double, Document> topk) throws IOException;

    /**
     * @return an instance of the AnnIndex class used.
     * @throws ClassNotFoundException 
     */
    public abstract AnnIndex getIndex() throws ClassNotFoundException;
}
