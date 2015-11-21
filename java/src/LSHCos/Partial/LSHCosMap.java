package LSHCos.Partial;

import TestGenericMR.*;
import TestGeneric.AnnIndex;
import TestGeneric.CandidateList;
import TestGeneric.Candidate;
import TestGeneric.Document;
import TestGenericMR.SourceQueryPairInputFormat.Pair;
import io.github.htools.hadoop.ContextTools;
import io.github.htools.io.Datafile;
import io.github.htools.lib.Log;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;

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
public class LSHCosMap extends TestGenericMap {

    public static final Log log = new Log(LSHCosMap.class);
    AnnLSHCos index;
    
    @Override
    public void setup(Context context) throws IOException {
        conf = ContextTools.getConfiguration(context);
        documentreader = new DocumentReaderTFIDF();
        Document.setSimilarityFunction(getSimilarityFunction());
        topk = TestGenericJob.getTopK(conf);
        try {
            index = new AnnLSHCos(getComparator(), conf);
        } catch (ClassNotFoundException ex) {
            log.fatalexception(ex, "setup");
        }
    }    
    
    @Override
    public void map(Integer key, Pair value, Context context) throws IOException, InterruptedException {
        // read all source documents and add to AnnIndex
        Datafile sourceDatafile = new Datafile(conf, value.sourcefile);
        ArrayList<Document> sourceDocuments = 
                documentreader.readDocuments(sourceDatafile);
        
        Datafile suspDatafile = new Datafile(conf, value.queryfile);
        ArrayList<Document> suspiciousDocuments = 
                documentreader.readDocuments(suspDatafile);
        index.set(sourceDocuments, suspiciousDocuments);
        
        // iterate over all suspicious documents
        for (Map.Entry<Document, long[]> query : index.queryIterator()) {
            Document suspiciousDocument = query.getKey();
            // retrieve the k most similar source documents from the index
            CandidateList topKSourceDocuments = new CandidateList(getTopK(), this.getComparator());
            index.getDocuments(topKSourceDocuments, query.getValue(), suspiciousDocument);

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
        TestGenericJob.addMeasuresCompared(context, Document.getSimilarityFunction().getComparisons());
        TestGenericJob.addGetDocumentsTime(context, AnnIndex.getGetDocumentsTime(), AnnIndex.getGetDocumentsCount());
        TestGenericJob.addFingerprintTime(context, AnnIndex.getGetFingerprintTime(), AnnIndex.getGetFingerprintCount());
        TestGenericJob.addSimilarityFunction(context, Document.getSimilarityFunction().getComparisonsTime(), 
                Document.getSimilarityFunction().getComparisons());
    }
}
