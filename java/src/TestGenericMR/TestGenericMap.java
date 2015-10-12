package TestGenericMR;

import SimilarityFile.SimilarityWritable;
import SimilarityFunction.SimilarityFunction;
import TestGeneric.Document;
import TestGenericMR.StringPairInputFormat.Value;
import io.github.htools.collection.TopKMap;
import io.github.htools.hadoop.Conf;
import io.github.htools.hadoop.ContextTools;
import io.github.htools.io.compressed.ArchiveFile;
import io.github.htools.io.compressed.ArchiveEntry;
import io.github.htools.lib.Log;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
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
public class TestGenericMap extends Mapper<Integer, Value, IntWritable, SimilarityWritable> {

    public static final Log log = new Log(TestGenericMap.class);
    Conf conf;
    SimilarityFunction similarityFunction;
    int topk;

    protected IntWritable outKey = new IntWritable();
    protected SimilarityWritable outValue = new SimilarityWritable();

    @Override
    public void setup(Context context) throws IOException {
        conf = ContextTools.getConfiguration(context);
        similarityFunction = TestGenericJob.getSimilarityFunction(conf);
        topk = TestGenericJob.getTopK(conf);
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
    
    @Override
    public void map(Integer key, Value value, Context context) throws IOException, InterruptedException {
        ArrayList<Document> sourceDocuments = readDocuments(value.sourcefile);
        for (Document suspiciousDocument : iterableDocuments(value.suspiciousfile)) {
            // retrieve the k-most similar source documents to the suspicious document
            TopKMap<Double, Document> topk = new TopKMap(getTopK());
            for (Document sourceDocument : sourceDocuments) {
                double similarity = similarity(sourceDocument, suspiciousDocument);
                topk.add(similarity, sourceDocument);
            }
            
            // write the topk to the reducer
            outKey.set(suspiciousDocument.getId());
            outValue.id = suspiciousDocument.getId();
            for (Map.Entry<Double, Document> entry : topk) {
                double similarity = entry.getKey();
                Document sourceDocument = entry.getValue();
                outValue.source = sourceDocument.getId();
                outValue.score = similarity(sourceDocument, suspiciousDocument);
                context.write(outKey, outValue);
            }
        }
    }
    
    public void cleanup(Context context) {
        TestGenericJob.countComparison(context, similarityFunction.getComparisons());
    }

    /**
     * @param documentFilename
     * @return an ArrayList of Documents read from an ArchiveFile on HDFS with
     * the name documentFilename
     * @throws IOException
     */
    public ArrayList<Document> readDocuments(String documentFilename) throws IOException {
        ArrayList<Document> documents = new ArrayList();
        for (Document document : iterableDocuments(documentFilename)) {
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
    public Iterable<Document> iterableDocuments(String documentFilename) throws IOException {
        ArchiveFile archiveFile = ArchiveFile.getReader(getConf(), documentFilename);
        return new DocumentIterator(archiveFile);
    }

    static class DocumentIterator implements Iterable<Document>, Iterator<Document> {

        Iterator<ArchiveEntry> iterator;

        DocumentIterator(ArchiveFile file) {
            iterator = file;
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
                    return new Document(next);
                }
            } catch (IOException ex) {
            }
            return null;
        }

    }

    public Configuration getConf() {
       return conf;
    }
}
