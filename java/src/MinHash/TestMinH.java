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
public class TestMinH extends TestGeneric {

    public static Log log = new Log(TestMinH.class);

    public TestMinH(HPath sourcePath, HPath suspiciousPath, Datafile similarityFile) throws IOException, ClassNotFoundException {
        setupOutput(similarityFile);
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
        return new AnnMinHash(new CosineSimilarity(), 240, 5);
    }
    
    public static void main(String[] args) throws IOException, ClassNotFoundException {
        ArgsParser ap = new ArgsParser(args, "source suspicious output");
        FSPath source = new FSPath(ap.get("source"));
        FSPath suspicious = new FSPath( ap.get("suspicious"));
        Datafile output = new Datafile(ap.get("output"));
        new TestMinH(source, suspicious, output);
    }
}
