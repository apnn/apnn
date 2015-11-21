package CosEst;

import SimilarityFile.SimilarityWritable;
import TestGeneric.AnnIndex;
import TestGeneric.CandidateList;
import TestGeneric.Document;
import io.github.htools.collection.HashMapDouble;
import io.github.htools.collection.TopKMap;
import io.github.htools.fcollection.FHashMapList;
import io.github.htools.fcollection.FHashMapObjectDouble;
import io.github.htools.lib.Log;
import io.github.htools.type.KV;
import io.github.htools.type.TermVectorDouble;
import it.unimi.dsi.fastutil.objects.Object2DoubleMap;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import java.util.Comparator;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;

/**
 * An ANN that uses the top-k n-tfidf terms per document for indexing and estimating 
 * the cosine similarity between a query document and the documents in the index.
 * n-tfidf = tf * idf / ||D|| (normalized by the length of the original document vector) 
 * @author Jeroen
 */
public class AnnCosEst extends AnnIndex<FHashMapObjectDouble<String>> {

    public static Log log = new Log(AnnCosEst.class);
    // number of terms to use to represent a document (top-k)
    protected int k;
    // inverted list per term, each entry being a Document and the n-tfidf
    protected FHashMapList<String, KV<Document, Double>> mapTerms;
    
    public AnnCosEst(
            Comparator<SimilarityWritable> comparator,
            int termssize) throws ClassNotFoundException {
        super(comparator);
        initialize(termssize);
    }

    public AnnCosEst(Comparator<SimilarityWritable> comparator, Configuration conf) throws ClassNotFoundException {
        this(comparator, CosEstJob.getTermsSize(conf));
    }

    private void initialize(int shingleSize) {
        this.k = shingleSize;
        mapTerms = new FHashMapList(1000000);
        // set initial size to prevent rehashing too often
    }

    @Override
    protected void addDocument(Document document, FHashMapObjectDouble<String> shortVector) {
        for (Object2DoubleMap.Entry<String> entry : shortVector.object2DoubleEntrySet()) {
            mapTerms.add(entry.getKey(), new KV<Document, Double>(document, entry.getDoubleValue()));
        }
    }

    @Override
    protected void getDocuments(CandidateList candidates, 
            FHashMapObjectDouble<String> shortVector, Document document) {
        HashMapDouble<Document> docCount = new HashMapDouble();
        this.countDocCodepoints += shortVector.size(); //
        for (Object2DoubleMap.Entry<String> fpentry : shortVector.object2DoubleEntrySet()) {
            double tfidf = fpentry.getDoubleValue();
            ObjectArrayList<KV<Document, Double>> list = mapTerms.get(fpentry.getKey());
            if (list != null) {
                for (KV<Document, Double> doc : list) {
                    log.info("%s %s %s %f %f", document.docid, fpentry.getKey(), doc.key.docid, tfidf, doc.value);
                    docCount.add(doc.key, doc.value * tfidf);
                }
            }
        }
        for (Map.Entry<Document, Double> entry : docCount.entrySet()) {
            double estimatedSimilarity = entry.getValue();
            candidates.add(entry.getKey(), estimatedSimilarity); // todo add estimation similarity
        }
    }

    @Override
    protected FHashMapObjectDouble<String> getFingerprint(Document document) {
        // returns a 'shortVector' of the top-k n-tfidf terms
        // take top-k tfidf terms
        TopKMap<Double, String> topk = new TopKMap(this.k);
        topk.addInvert((TermVectorDouble)document.getModel());
        
        // convert these into a <String, Double> map.
        FHashMapObjectDouble<String> result = new FHashMapObjectDouble();
        for (Map.Entry<Double, String> entry : topk) {
            double ntfidf = entry.getKey() / document.getModel().magnitude();
            result.add(entry.getValue(), ntfidf);
        }
        return result;
    }
}
