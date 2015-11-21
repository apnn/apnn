package Canopy;

import SimilarityFile.MeasureSimilarity;
import SimilarityFile.SimilarityWritable;
import SimilarityFunction.CosineSimilarityTFIDF;
import TestGeneric.AnnIndex;
import TestGeneric.CandidateList;
import TestGeneric.Document;
import TestGeneric.TestGeneric;
import TestGenericMR.DocumentReader;
import TestGenericMR.DocumentReaderTFIDF;
import io.github.htools.io.Datafile;
import io.github.htools.io.FSPath;
import io.github.htools.io.HPath;
import io.github.htools.lib.ArgsParser;
import java.io.IOException;
import java.util.Comparator;

/**
 * An ANN that uses the top-k n-tfidf terms per document for indexing and
 * estimating the cosine similarity between a query document and the documents
 * in the index. n-tfidf = tf * idf / ||D|| (normalized by the length of the
 * original document vector)
 *
 * @author Jeroen
 */
public class TestCanopy extends TestGeneric {
    
    TestCanopy(Comparator<SimilarityWritable> comparator, HPath source, 
            HPath query, Datafile result) throws IOException, ClassNotFoundException {
        super(comparator);
        Document.setSimilarityFunction(new CosineSimilarityTFIDF(null));
        loadSourceDocuments(source);
        setupOutput(result);
        streamQueryDocuments(query);
        closeOutput();
    }
            
    public DocumentReader getDocumentReader() {
        return new DocumentReaderTFIDF();
    }
    
    @Override
    public void processTopKNN(Document query, CandidateList topk) throws IOException {
        this.writeSimilarities(query, topk);
    }

    @Override
    public AnnIndex getIndex(Comparator<SimilarityWritable> comparator) throws ClassNotFoundException {
        return new AnnCanopyCosine(comparator, 0.9, 0.9, 20);
    }
    
    public static void main(String[] args) throws IOException, ClassNotFoundException {
        ArgsParser ap = new ArgsParser(args, "source query result");
        new TestCanopy(MeasureSimilarity.singleton, 
                new FSPath(ap.get("source")),
                new FSPath(ap.get("query")),
                ap.getDatafile("result"));
    }
}
