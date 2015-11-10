package CosEst;

import SimilarityFile.SimilarityWritable;
import SimilarityFunction.CosineSimilarityTFIDF;
import SimilarityFunction.SimilarityFunction;
import TestGeneric.AnnIndex;
import TestGeneric.CandidateList;
import TestGeneric.Document;
import Vocabulary.Idf;
import io.github.htools.collection.HashMapDouble;
import io.github.htools.collection.TopKMap;
import io.github.htools.fcollection.FHashMapList;
import io.github.htools.fcollection.FHashMapObjectDouble;
import io.github.htools.lib.CollectionTools;
import io.github.htools.lib.Log;
import io.github.htools.type.KV;
import io.github.htools.type.TermVectorDouble;
import it.unimi.dsi.fastutil.objects.Object2DoubleMap;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import java.util.Comparator;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;

/**
 * @author Jeroen
 */
public class AnnCosEst extends AnnIndex<FHashMapObjectDouble<String>> {

    public static Log log = new Log(AnnCosEst.class);
    protected int termssize;
    protected FHashMapList<String, KV<Document, Double>> mapTerms;
    protected Idf idf;

    public AnnCosEst(SimilarityFunction similarityFunction,
            Comparator<SimilarityWritable> comparator,
            int shingleSize) throws ClassNotFoundException {
        super(similarityFunction, comparator);
        initialize(shingleSize);
    }

    public AnnCosEst(SimilarityFunction function, Comparator<SimilarityWritable> comparator, Configuration conf) throws ClassNotFoundException {
        this(function, comparator, CosEstJob.getTermsSize(conf));
    }

    private void initialize(int shingleSize) {
        this.termssize = shingleSize;
        mapTerms = new FHashMapList(1000000);
        idf = ((CosineSimilarityTFIDF) this.similarityFunction).idf;
        // set initial size to prevent rehashing too often
    }

    @Override
    protected void addDocument(Document document, FHashMapObjectDouble<String> fp) {
        for (Object2DoubleMap.Entry<String> entry : fp.object2DoubleEntrySet()) {
            mapTerms.add(entry.getKey(), new KV<Document, Double>(document, entry.getDoubleValue()));
        }
    }

    @Override
    protected void getDocuments(CandidateList candidates, FHashMapObjectDouble<String> fp, Document document) {
        HashMapDouble<Document> docCount = new HashMapDouble();
        //log.info("fp size %d %d", document.docid, fp.size());
        this.countDocCodepoints += fp.size();
        log.info("%d %s", document.docid, fp);
        for (Object2DoubleMap.Entry<String> fpentry : fp.object2DoubleEntrySet()) {
            double tfidf = fpentry.getDoubleValue();
            ObjectArrayList<KV<Document, Double>> list = mapTerms.get(fpentry.getKey());
            if (list != null) {
                for (KV<Document, Double> doc : list) {
                    log.info("%d %s %d %f %f", document.docid, fpentry.getKey(), doc.key.docid, tfidf, doc.value);
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
        FHashMapObjectDouble<String> result = new FHashMapObjectDouble();
        TopKMap<Double, String> topk = new TopKMap(this.termssize);
        topk.addInvert((TermVectorDouble)document.getModel());
        for (Map.Entry<Double, String> entry : topk) {
            result.add(entry.getValue(), entry.getKey() / document.getModel().magnitude());
        }
        return result;
    }
}
