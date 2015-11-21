package LSHCos.Partial;

import SimilarityFile.SimilarityWritable;
import TestGeneric.Candidate;
import TestGeneric.CandidateList;
import TestGeneric.Document;
import io.github.htools.collection.ArrayMap;
import io.github.htools.fcollection.FHashMap;
import io.github.htools.lib.Log;
import io.github.htools.lib.RandomTools;
import io.github.htools.type.TermVectorDouble;
import it.unimi.dsi.fastutil.objects.Object2DoubleMap;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;

/**
 * @author Jeroen
 */
public class AnnLSHCos {

    public static Log log = new Log(AnnLSHCos.class);
    RandomTools.RandomGenerator random = RandomTools.createGenerator(0);
    protected Comparator<SimilarityWritable> comparator;
    protected int numHyperplanes = 100;
    protected int fingerprintSize = numHyperplanes / 64;
    HashSet<String> vocabulary;
    FHashMap<String, float[]> randomPlane;
    ArrayMap<Document, long[]> index = new ArrayMap();
    ArrayMap<Document, long[]> queries = new ArrayMap();

    public AnnLSHCos(Comparator<SimilarityWritable> comparator, Configuration conf) throws ClassNotFoundException {
        this.comparator = comparator;
        this.numHyperplanes = LSHCosJob.getNumHyperplanes(conf);
        fingerprintSize = 1 + (numHyperplanes - 1) / 64;
    }

    public void createRandomPlanes64() {
        randomPlane = new FHashMap(vocabulary.size());
        for (String term : vocabulary) {
            float[] weights = new float[64];
            for (int i = 0; i < 64; i++) {
                weights[i] = (float)random.getStdNormal();
            }
            randomPlane.put(term, weights);
        }
    }
    
    public Iterable<Map.Entry<Document, long[]>> queryIterator() {
        return queries;
    }
    
    private void setVocabulary() {
        vocabulary = new HashSet(10000);
        for (Document d : index.keySet()) {
            vocabulary.addAll(d.getModel().keySet());
        }
        for (Document d : queries.keySet())
            vocabulary.addAll(d.getModel().keySet());
    }
    
    public void set(ArrayList<Document> source, ArrayList<Document> queries) {
        random = RandomTools.createGenerator(0);
        index = createEmptyFingerPrints(source);
        this.queries = createEmptyFingerPrints(queries);
        setVocabulary();
        for (int part = 0; part < fingerprintSize; part++) {
            createRandomPlanes64();
            int bits = part < fingerprintSize?64:numHyperplanes % 64;
            createFingerprint(index, part, bits);
            createFingerprint(this.queries, part, bits);
        }
    }
    
    public void createFingerprint(ArrayMap<Document, long[]> map, int part, int bits) {
        for (Map.Entry<Document, long[]> entry : map) {
            long fp = getFingerprint(entry.getKey(), bits);
            entry.getValue()[part] = fp;
        }
    }
    
    public ArrayMap<Document, long[]> createEmptyFingerPrints(ArrayList<Document> documents) {
        ArrayMap<Document, long[]> result = new ArrayMap();
        for (Document d : documents) {
            result.add(d, new long[fingerprintSize]);
        }
        return result;
    }
    
    protected void addDocument(Document document, long[] fp) {
        index.add(document, fp);
        document.clearContent();
        document.clearTerms();
    }

    protected void getDocuments(CandidateList candidates, long[] fp, Document document) {
        for (Map.Entry<Document, long[]> entry : index) {
            long[] entryFp = entry.getValue();
            int dissim = 0;
            for (int i = 0; i < fingerprintSize; i++) {
                dissim += Long.bitCount(fp[i] ^ entryFp[i]);
            }
            //if (document.docid == 1) {
                //long a = fp[0] ^ entryFp[0];
                //long b = fp[1] ^ entryFp[1];
//                log.printf("%5d %64s\n%5d %64s\n%5d %64s", document.docid, Long.toBinaryString(fp[0]), 
//                                                         entry.getKey().docid, Long.toBinaryString(entryFp[0]), 
//                                                         dissim, Long.toBinaryString(a));
//                log.printf("%5d %64s\n%5d %64s\n%5d %64s\n", document.docid, Long.toBinaryString(fp[1]), 
//                                                         entry.getKey().docid, Long.toBinaryString(entryFp[1]), 
//                                                         dissim, Long.toBinaryString(b));
            //}
            double similarity = Math.cos((1 - (numHyperplanes - dissim) / (double) numHyperplanes) * Math.PI);
            candidates.add(entry.getKey(), similarity);
        }
        for (Candidate c : candidates) {
            c.measureSimilarity = document.similarity(c.document);
        }
    }

    protected long getFingerprint(Document document, int bits) {
        double[] offsetToHyperplane = new double[bits];
        for (Object2DoubleMap.Entry<String> entry : ((TermVectorDouble) document.getModel()).object2DoubleEntrySet()) {
            float[] planeWeights = randomPlane.get(entry.getKey());
            if (planeWeights != null) {
                double tfidf = entry.getDoubleValue();
                for (int h = 0; h < bits; h++) {
                    offsetToHyperplane[h] += planeWeights[h] * tfidf;
                }
            }
        }
        long fingerprint = 0;
        for (int i = 0; i < bits; i++) {
            if (offsetToHyperplane[i] > 0) {
                fingerprint |= (1l << i);
            }
        }
        return fingerprint;
    }
}
