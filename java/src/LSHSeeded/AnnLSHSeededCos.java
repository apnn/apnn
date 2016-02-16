package LSHSeeded;

import LSHCos.LSHCosJob;
import SimilarityFile.SimilarityWritable;
import TestGeneric.AnnIndex;
import TestGeneric.Candidate;
import TestGeneric.CandidateList;
import TestGeneric.Document;
import io.github.htools.collection.ArrayMap;
import io.github.htools.io.Datafile;
import io.github.htools.lib.Log;
import io.github.htools.type.TermVectorDouble;
import org.apache.hadoop.conf.Configuration;

import java.util.Comparator;
import java.util.Map;

/**
 * @author Jeroen
 */
public class AnnLSHSeededCos extends AnnIndex<long[]> {

    public static Log log = new Log(AnnLSHSeededCos.class);
    protected TermVectorDouble[] lsh;
    protected int numHyperplanes = 100;
    protected int fingerprintSize = numHyperplanes / 64;
    ArrayMap<Document, long[]> index = new ArrayMap();
    ArrayMap<Document, long[]> queries = new ArrayMap();

    public AnnLSHSeededCos(Comparator<SimilarityWritable> comparator, Configuration conf) throws ClassNotFoundException {
        super(comparator);
        this.numHyperplanes = LSHCosJob.getNumHyperplanes(conf);
        fingerprintSize = 1 + (numHyperplanes - 1) / 64;
        readLsh(new Datafile(conf, conf.get("lsh")));
    }

    public void readLsh(Datafile df) {
        lsh = new TermVectorDouble[this.numHyperplanes];
        int vectornr = 0;

        for (String line : df.readLines()) {
            if (vectornr < numHyperplanes) {
                String terms[] = line.split("\\s");
                TermVectorDouble newLsh = new TermVectorDouble(terms.length);
                for (String term : terms) {
                    String part[] = term.split(":");
                    newLsh.put(part[0], Double.parseDouble(part[1]));
                }
                lsh[vectornr++] = newLsh;
            }
        }
    }

    protected void addDocument(Document document, long[] fp) {
        index.add(document, fp);
        document.clearContent();
        document.clearTerms();
    }

    protected void getDocuments(CandidateList candidates, long[] fp, Document document) {
        for (Map.Entry<Document, long[]> entry : index) {
            if (document != entry.getKey()) {
                long[] entryFp = entry.getValue();
                int dissim = 0;
                for (int i = 0; i < fingerprintSize; i++) {
                    dissim += Long.bitCount(fp[i] ^ entryFp[i]);
                }

                double similarity = Math.cos((1 - (numHyperplanes - dissim) / (double) numHyperplanes) * Math.PI);
                candidates.add(entry.getKey(), similarity);
            }
        }
        for (Candidate c : candidates) {
            c.measureSimilarity = document.similarity(c.document);
        }
    }

    public long[] getFingerprintSource(Document document) {
        long[] fp = new long[fingerprintSize];
        for (int i = 0; i < numHyperplanes; i++) {
            if (lsh[i].dotproduct(document.getModel()) > 0)
                fp[i / 64] |= (1l << i);
        }
        return fp;
    }
}
