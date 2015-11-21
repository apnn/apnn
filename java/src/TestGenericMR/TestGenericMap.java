package TestGenericMR;

import SimilarityFile.IndexSimilarity;
import SimilarityFile.SimilarityWritable;
import SimilarityFunction.SimilarityFunction;
import TestGeneric.AnnIndex;
import TestGeneric.CandidateList;
import TestGeneric.Candidate;
import TestGeneric.Document;
import TestGenericMR.SourceQueryPairInputFormat.Pair;
import io.github.htools.hadoop.Conf;
import io.github.htools.hadoop.ContextTools;
import io.github.htools.io.Datafile;
import io.github.htools.lib.Log;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
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
public class TestGenericMap extends Mapper<Integer, Pair, Text, Candidate> {

    public static final Log log = new Log(TestGenericMap.class);
    protected Conf conf;
    protected DocumentReader documentreader;
    protected AnnIndex index;
    protected int topk;

    protected Text outKey = new Text();

    @Override
    public void setup(Context context) throws IOException {
        conf = ContextTools.getConfiguration(context);
        documentreader = getDocumentReader();
        topk = TestGenericJob.getTopK(conf);
        index = TestGenericJob.getAnnIndex(getComparator(), getConf());
        Document.setSimilarityFunction(getSimilarityFunction());
    }
    
    public DocumentReader getDocumentReader() {
        return new DocumentReaderTerms();
    }

    public SimilarityFunction getSimilarityFunction() {
        return TestGenericJob.getSimilarityFunction(conf);
    }
    
    public int getTopK() {
        return topk;
    }

    public Comparator<SimilarityWritable> getComparator() {
        return IndexSimilarity.singleton;
    } 
    
    @Override
    public void map(Integer key, Pair value, Context context) throws IOException, InterruptedException {
        // read all source documents and add to AnnIndex
        Datafile sourceDatafile = new Datafile(conf, value.sourcefile);
        ArrayList<Document> sourceDocuments = 
                documentreader.readDocuments(sourceDatafile);
        for (Document sourceDocument : sourceDocuments) {
            index.add(sourceDocument);
        }
        index.finishIndex();

        // iterate over all suspicious documents
        Datafile queryDF = new Datafile(conf, value.queryfile);
        for (Document query : documentreader.iterableDocuments(queryDF)) {
            // retrieve the k most similar source documents from the index
            CandidateList topKSourceDocuments = index.getNNs(query, getTopK());

            // write the topk to the reducer
            outKey.set(query.getId());
            for (Candidate candidate : topKSourceDocuments) {
                candidate.id = query.docid;
                
                // send to reducer
                context.write(outKey, candidate);
            }
        }
    }
    
    @Override
    public void cleanup(Context context) {
        TestGenericJob.addMeasuresCompared(context, Document.getSimilarityFunction().getComparisons());
        TestGenericJob.addGetDocumentsTime(context, AnnIndex.getGetDocumentsTime(), AnnIndex.getGetDocumentsCount());
        TestGenericJob.addGetDocCodepoints(context, index.countDocCodepoints);
        TestGenericJob.addGetDocComparedCodepoints(context, index.countComparedDocCodepoints);
        TestGenericJob.addFingerprintTime(context, AnnIndex.getGetFingerprintTime(), AnnIndex.getGetFingerprintCount());
        TestGenericJob.addSimilarityFunction(context, Document.getSimilarityFunction().getComparisonsTime(), 
                Document.getSimilarityFunction().getComparisons());
        index.cleanup(context);
    }

    public Configuration getConf() {
       return conf;
    }
}
