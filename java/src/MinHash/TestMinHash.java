package MinHash;

import SimilarityFile.MeasureSimilarity;
import SimilarityFile.SimilarityWritable;
import TestGeneric.TestGeneric;
import TestGeneric.Document;
import SimilarityFunction.CosineSimilarityTFIDF;
import TestGeneric.AnnIndex;
import TestGeneric.CandidateList;
import io.github.htools.collection.TopKMap;
import io.github.htools.io.Datafile;
import io.github.htools.io.FSPath;
import io.github.htools.io.HPath;
import io.github.htools.lib.ArgsParser;
import io.github.htools.lib.Log;
import java.io.IOException;
import java.util.Comparator;

/**
 * This is an example of how you could setup a non-Hadoop run on the collections.
 * Note that for this example on PAN11 this requires more than 4G memory.
 * @author jeroen
 */
public class TestMinHash extends TestGeneric {

    public static Log log = new Log(TestMinHash.class);

    public TestMinHash(HPath sourcePath, 
            HPath suspiciousPath,
            Datafile vocabulary,
            Datafile resultFile) 
            throws IOException, ClassNotFoundException {
        super(vocabulary, MeasureSimilarity.singleton);
        setupOutput(resultFile);
        loadSourceDocuments(sourcePath);
        streamQueryDocuments(suspiciousPath);
        closeOutput();
        log.info("comparisons %d", Document.getSimilarityFunction().getComparisons());
    }

    @Override
    public void processTopKNN(Document suspiciousDoc, CandidateList topk) throws IOException {
        this.writeSimilarities(suspiciousDoc, topk);
    }

    
   @Override
    public AnnIndex getIndex(Comparator<SimilarityWritable> comparator) throws ClassNotFoundException {
        // this implementation is fixed for now with hashFunctions=240 and bandwidth=2
        return new AnnMinHash(comparator, 100, 1);
    }
    
    public static void main(String[] args) throws IOException, ClassNotFoundException {
        ArgsParser ap = new ArgsParser(args, "sourcepath suspiciouspath vocabulary output");
        FSPath source = new FSPath(ap.get("sourcepath"));
        FSPath suspicious = new FSPath( ap.get("suspiciouspath"));
        Datafile vocabulary = new Datafile(ap.get("vocabulary"));
        Datafile output = new Datafile(ap.get("output"));
        new TestMinHash(source, suspicious, vocabulary, output);
    }
}
