package BruteForce;

import SimilarityFile.SimilarityWritable;
import SimilarityFunction.SimilarityFunction;
import TestGeneric.AnnIndex;
import TestGeneric.Candidate;
import TestGeneric.CandidateList;
import TestGeneric.Document;
import io.github.htools.lib.Log;
import java.util.ArrayList;
import java.util.Comparator;
import org.apache.hadoop.conf.Configuration;

/**
 * @author Jeroen
 */
public class AnnBruteForce extends AnnIndex {

    public static Log log = new Log(AnnBruteForce.class);
    ArrayList<Document> sourceDocuments = new ArrayList();
    
    public AnnBruteForce(SimilarityFunction similarityFunction, Comparator<SimilarityWritable> comparator) throws ClassNotFoundException {
        super(similarityFunction, comparator);
    }
    
    public AnnBruteForce(SimilarityFunction function, Comparator<SimilarityWritable> comparator, Configuration conf) throws ClassNotFoundException {
        this(function, comparator);
    }
    
    @Override
    protected void addDocument(Document document, Object minHash) {
        sourceDocuments.add(document);
    }

    @Override
    protected void getDocuments(CandidateList list, Object fingerprint, Document document) {
       for (Document sourceDocument : sourceDocuments) {
           double similarity = similarityFunction.similarity(document, sourceDocument);
           list.add(sourceDocument, similarity);
       }
    }

    @Override
    protected void assignMeasureSimilarity(CandidateList candidates, Document document) {
        for (Candidate candidate : candidates) {
            candidate.measureSimilarity = candidate.indexSimilarity;
            candidate.id = document.docid;
        }
    }    
    
    @Override
    protected Object getFingerprint(Document document) {
        return null;
    }
}
