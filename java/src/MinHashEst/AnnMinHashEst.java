package MinHashEst;

import MinHash.AnnMinHash;
import MinHash.MinHashJob;
import SimilarityFile.SimilarityWritable;
import SimilarityFunction.SimilarityFunction;
import TestGeneric.CandidateList;
import TestGeneric.Document;
import io.github.htools.collection.HashMapInt;
import io.github.htools.collection.TopKMap;
import io.github.htools.fcollection.FHashMapIntList;
import io.github.htools.lib.CollectionTools;
import io.github.htools.lib.Log;
import io.github.htools.type.TermVectorDouble;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import java.util.Comparator;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;

/**
 * @author Jeroen
 */
public class AnnMinHashEst extends AnnMinHash {

    public static Log log = new Log(AnnMinHashEst.class);

    public AnnMinHashEst(SimilarityFunction similarityFunction, Comparator<SimilarityWritable> comparator, int numHashFunctions, int bandwidth) throws ClassNotFoundException {
        super(similarityFunction, comparator, numHashFunctions, bandwidth);
    }

    public AnnMinHashEst(SimilarityFunction function, Comparator<SimilarityWritable> comparator, Configuration conf) throws ClassNotFoundException {
        this(function, comparator, MinHashJob.getNumHashFunctions(conf), MinHashJob.getBandwidth(conf));
    }

    @Override
    protected void getDocuments(CandidateList candidates, int[] minHash, Document query) {
        if (query.docid == 1) {
            TopKMap<Double, String> topterms = new TopKMap(50);
            for (Map.Entry<String, Double> entry : ((TermVectorDouble) query.getModel()).entrySet()) {
                topterms.add(entry.getValue(), entry.getKey());
            }
            log.info("doc 1 %d %s", query.getTerms().size(), topterms.sorted());
        }
        HashMapInt<Document> documents = new HashMapInt();
        for (int i = 0; i < minHash.length; i++) {
            FHashMapIntList<Document> minHashtable = minHashTables[i];
            ObjectArrayList<Document> list = minHashtable.get(minHash[i]);
            if (list != null) {
                documents.addAll(list);
            }
        }
        for (Map.Entry<Document, Integer> entry : documents.entrySet()) {
            double indexSimilarity = entry.getValue() / (double) minHash.length;
            candidates.add(entry.getKey(), indexSimilarity);
            if (query.docid == 1) {
                log.info("%d %f", entry.getKey().docid, indexSimilarity);
                if (entry.getKey().docid == 9168) {
                    TopKMap<Double, String> topterms = new TopKMap(50);
                    for (Map.Entry<String, Double> entry2 : ((TermVectorDouble) entry.getKey().getModel()).entrySet()) {
                        topterms.add(entry2.getValue(), entry2.getKey());
                    }
                    log.info("%d %d %s", entry.getKey().docid, entry.getKey().getTerms().size(), topterms);
                    ((TermVectorDouble)entry.getKey().getModel()).cossimDebug((TermVectorDouble)query.getModel());
                }
            }
        }
    }
}
