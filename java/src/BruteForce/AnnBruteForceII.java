package BruteForce;

import SimilarityFile.SimilarityWritable;
import TestGeneric.AnnIndex;
import TestGeneric.Candidate;
import TestGeneric.CandidateList;
import TestGeneric.Document;
import io.github.htools.collection.ArrayMap;
import io.github.htools.collection.HashMapDouble;
import io.github.htools.collection.HashMapList3;
import io.github.htools.lib.Log;
import io.github.htools.type.TermVectorDouble;
import org.apache.hadoop.conf.Configuration;

import java.util.Comparator;
import java.util.Map;

/**
 * @author Jeroen
 */
public class AnnBruteForceII extends AnnIndex<TermVectorDouble> {

    public static Log log = new Log(AnnBruteForceII.class);
    HashMapList3<String, Document, Double> ii = new HashMapList3();

    public AnnBruteForceII(Comparator<SimilarityWritable> comparator) throws ClassNotFoundException {
        super(comparator);
    }

    public AnnBruteForceII(Comparator<SimilarityWritable> comparator, Configuration conf) throws ClassNotFoundException {
        this(comparator);
    }

    @Override
    protected void addDocument(Document document, TermVectorDouble model) {
        for (Map.Entry<String, Double> entry : model.entrySet()) {
            ii.getList(entry.getKey()).add(document, entry.getValue());
        }
    }

    @Override
    protected void getDocuments(CandidateList list, TermVectorDouble model, Document document) {
        HashMapDouble<Document> map = new HashMapDouble();
        for (Map.Entry<String, Double> entry : model.entrySet()) {
            ArrayMap<Document, Double> entries = ii.get(entry.getKey());
            if (entries != null) {
                for (Map.Entry<Document, Double> doc : entries.entrySet()) {
                    map.add(doc.getKey(), doc.getValue() * entry.getValue());
                }
            }
        }
        ArrayMap<Double, Document> sorted = ArrayMap.invert(map.entrySet());
        for (Map.Entry<Double, Document> entry : sorted.descending()) {
            list.add(entry.getValue(), entry.getKey());
            if (list.size() >= list.getK())
                break;
        }
    }

    @Override
    protected void assignMeasureSimilarity(CandidateList candidates, Document document) {
        for (Candidate candidate : candidates) {
            candidate.measureSimilarity = candidate.indexSimilarity;
            candidate.query = document.docid;
        }
    }

    @Override
    public TermVectorDouble getFingerprintSource(Document document) {
        return document.getModel();
    }
}
