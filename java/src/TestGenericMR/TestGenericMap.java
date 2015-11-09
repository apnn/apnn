package TestGenericMR;

import SimilarityFile.MeasureSimilarity;
import SimilarityFile.SimilarityWritable;
import SimilarityFunction.SimilarityFunction;
import TestGeneric.AnnIndex;
import TestGeneric.CandidateList;
import TestGeneric.Candidate;
import TestGeneric.Document;
import TestGenericMR.StringPairInputFormat.Value;
import io.github.htools.hadoop.Conf;
import io.github.htools.hadoop.ContextTools;
import io.github.htools.io.compressed.ArchiveFile;
import io.github.htools.io.compressed.ArchiveEntry;
import io.github.htools.lib.Log;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Every map receives one Value as input, that contains a unique combination of
 * a filename with a block of suspicious documents and a filename with a block
 * of source documents. The default operation is to do a brute force comparison
 * by reading all source documents into memory, and
 * then the suspicious documents are inspected one-at-a-time for matches in the
 * index. The k-most similar source documents are send to the reducer. This
 * generic mapper can be configured by setting the index, similarity function
 * and k in the job, or overridden to add functionality.
 *
 * @author jeroen
 */
public class TestGenericMap extends Mapper<Integer, Value, IntWritable, Candidate> {

    public static final Log log = new Log(TestGenericMap.class);
    Conf conf;
    AnnIndex index;
    SimilarityFunction similarityFunction;
    int topk;

    protected IntWritable outKey = new IntWritable();

    @Override
    public void setup(Context context) throws IOException {
        conf = ContextTools.getConfiguration(context);
        similarityFunction = TestGenericJob.getSimilarityFunction(conf);
        topk = TestGenericJob.getTopK(conf);
        Document.setTokenizer(conf);
        index = TestGenericJob.getAnnIndex(getSimilarityFunction(), getComparator(), getConf());
    }

    public SimilarityFunction getSimilarityFunction() {
        return similarityFunction;
    }
    
    public double similarity(Document a, Document b) {
        return similarityFunction.similarity(a, b);
    }
    
    public int getTopK() {
        return topk;
    }

    public Comparator<SimilarityWritable> getComparator() {
        return MeasureSimilarity.singleton;
    } 
    
    @Override
    public void map(Integer key, Value value, Context context) throws IOException, InterruptedException {
        // read all source documents and add to AnnIndex
        ArrayList<Document> sourceDocuments = readDocuments(conf, value.sourcefile, getSimilarityFunction());
        for (Document sourceDocument : sourceDocuments) {
            index.add(sourceDocument);
        }

        // iterate over all suspicious documents
        for (Document suspiciousDocument : iterableDocuments(conf, value.suspiciousfile, getSimilarityFunction())) {
            // retrieve the k most similar source documents from the index
            CandidateList topKSourceDocuments = index.getNNs(suspiciousDocument, getTopK());

            // write the topk to the reducer
            outKey.set(suspiciousDocument.getId());
            for (Candidate candidate : topKSourceDocuments) {
                candidate.id = suspiciousDocument.docid;
                
                // send to reducer
                context.write(outKey, candidate);
            }
        }
    }
    
    @Override
    public void cleanup(Context context) {
        TestGenericJob.addMeasuresCompared(context, similarityFunction.getComparisons());
        TestGenericJob.addGetDocumentsTime(context, AnnIndex.getGetDocumentsTime(), AnnIndex.getGetDocumentsCount());
        TestGenericJob.addGetDocCodepoints(context, index.countDocCodepoints);
        TestGenericJob.addGetDocComparedCodepoints(context, index.countComparedDocCodepoints);
        TestGenericJob.addFingerprintTime(context, AnnIndex.getGetFingerprintTime(), AnnIndex.getGetFingerprintCount());
        TestGenericJob.addSimilarityFunction(context, similarityFunction.getComparisonsTime(), similarityFunction.getComparisons());
        
    }

    /**
     * @param documentFilename
     * @return an ArrayList of Documents read from an ArchiveFile on HDFS with
     * the name documentFilename
     * @throws IOException
     */
    public static ArrayList<Document> readDocuments(Configuration conf, 
            String documentFilename, 
            SimilarityFunction function) throws IOException {
        
        ArrayList<Document> documents = new ArrayList();
        for (Document document : iterableDocuments(conf, documentFilename, function)) {
            // extract the docid from the filename (in the tar-file)
            // add to the map of documents
            documents.add(document);
        }
        return documents;
    }

    /**
     * An Iterable over the documents in an archiveFile on HDFS with the name
     * documentFilename
     *
     * @param documentFilename
     * @return
     * @throws IOException
     */
    public static Iterable<Document> iterableDocuments(Configuration conf, String documentFilename, SimilarityFunction function) throws IOException {
        ArchiveFile archiveFile = ArchiveFile.getReader(conf, documentFilename);
        return new DocumentIterator(archiveFile, function);
    }

    public static Document readDocument(ArchiveEntry entry) throws IOException {
        return Document.read(entry);
    }
    
    static class DocumentIterator implements Iterable<Document>, Iterator<Document> {

        Iterator<ArchiveEntry> iterator;
        SimilarityFunction similarityFunction;

        DocumentIterator(ArchiveFile file, SimilarityFunction function) {
            iterator = file;
            this.similarityFunction = function;
        }

        @Override
        public Iterator<Document> iterator() {
            return this;
        }

        @Override
        public boolean hasNext() {
            return iterator.hasNext();
        }

        @Override
        public Document next() {
            try {
                ArchiveEntry next = iterator.next();
                if (next != null) {
                    Document document = readDocument(next);
                    similarityFunction.reweight(document);
                    return document;
                }
            } catch (IOException ex) {
            }
            return null;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("Not supported.");
        }

    }

    public Configuration getConf() {
       return conf;
    }
}
