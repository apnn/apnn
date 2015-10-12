package MinHash;

import TestGeneric.TestGeneric;
import TestGeneric.Document;
import SimilarityFunction.CosineSimilarity;
import TestGeneric.AnnIndex;
import io.github.htools.collection.TopKMap;
import io.github.htools.io.Datafile;
import io.github.htools.io.FSPath;
import io.github.htools.io.HPath;
import io.github.htools.lib.ArgsParser;
import io.github.htools.lib.Log;
import java.io.IOException;

/**
 * This is an example of how you could setup a non-Hadoop run on the collections.
 * Note that for this example on PAN11 this requires more than 4G memory.
 * @author jeroen
 */
public class TestMinHash extends TestGeneric {

    public static Log log = new Log(TestMinHash.class);

    public TestMinHash(HPath sourcePath, 
            HPath suspiciousPath, 
            Datafile resultFile) 
            throws IOException, ClassNotFoundException {
        
        setupOutput(resultFile);
        loadSourceDocuments(sourcePath);
        streamSuspiciousDocuments(suspiciousPath);
        closeOutput();
    }

    @Override
    public void processTopKNN(Document suspiciousDoc, TopKMap<Double, Document> topk) throws IOException {
        this.writeSimilarities(suspiciousDoc, topk);
    }

    @Override
    public AnnIndex getIndex() throws ClassNotFoundException {
        // this implementation is fixed for now with hashFunctions=240 and bandwidth=2
        return new AnnMinHash(new CosineSimilarity(), 240, 2);
    }
    
    public static void main(String[] args) throws IOException, ClassNotFoundException {
        ArgsParser ap = new ArgsParser(args, "sourcepath suspiciouspath output");
        FSPath source = new FSPath(ap.get("sourcepath"));
        FSPath suspicious = new FSPath( ap.get("suspiciouspath"));
        Datafile output = new Datafile(ap.get("output"));
        new TestMinHash(source, suspicious, output);
    }
}
