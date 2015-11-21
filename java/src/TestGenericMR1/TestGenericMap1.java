package TestGenericMR1;

import TestGenericMR.*;
import SimilarityFile.MeasureSimilarity;
import SimilarityFile.SimilarityFile;
import SimilarityFile.SimilarityWritable;
import SimilarityFunction.SimilarityFunction;
import TestGeneric.AnnIndex;
import TestGeneric.CandidateList;
import TestGeneric.Candidate;
import TestGeneric.Document;
import io.github.htools.hadoop.Conf;
import io.github.htools.hadoop.ContextTools;
import io.github.htools.io.Datafile;
import io.github.htools.lib.Log;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * This variant does not map-reduce, but rather uses a single mapper to construct
 * an in-memory index and solve the query documents, allowing more memory allocation
 * than on a single machine.
 *
 * @author jeroen
 */
public class TestGenericMap1 extends Mapper<NullWritable, NullWritable, NullWritable, NullWritable> {

    public static final Log log = new Log(TestGenericMap1.class);
    protected Conf conf;
    protected DocumentReader documentreader;
    protected AnnIndex index;
    protected int topk;
    protected SimilarityFile similarityFile;

    @Override
    public void setup(Context context) throws IOException {
        conf = ContextTools.getConfiguration(context);
        documentreader = getDocumentReader();
        topk = TestGenericJob.getTopK(conf);
        index = TestGenericJob.getAnnIndex(getComparator(), getConf());
        Document.setSimilarityFunction(getSimilarityFunction());
        similarityFile = new SimilarityFile(new Datafile(conf, conf.get("output")));
        similarityFile.openWrite();
    }

    public DocumentReader getDocumentReader() {
        return new DocumentReaderTerms();
    }

    public SimilarityFunction getSimilarityFunction() {
        return TestGenericJob.getSimilarityFunction(conf);
    }

    public int getTopK() {
        return topk;
    }

    public Comparator<SimilarityWritable> getComparator() {
        return MeasureSimilarity.singleton;
    }

    @Override
    public void map(NullWritable key, NullWritable value, Context context) throws IOException, InterruptedException {
        log.info("%s", conf.getHDFSPath("source").getCanonicalPath());
        for (Datafile df : conf.getHDFSPath("source").getFiles()) {
            log.info("file %s", df.getCanonicalPath());
            for (Document sourceDocument : documentreader.readDocuments(df)) {
                index.add(sourceDocument);
            }
        }
        index.finishIndex();

        // iterate over all suspicious documents
        for (Datafile df : conf.getHDFSPath("query").getFiles()) {
            log.info("query %s", df.getCanonicalPath());
            for (Document suspiciousDocument : documentreader.iterableDocuments(df)) {
                // retrieve the k most similar source documents from the index
                CandidateList topKSourceDocuments = index.getNNs(suspiciousDocument, getTopK());
                ArrayList<Candidate> list = new ArrayList(topKSourceDocuments);

                Collections.sort(list, Collections.reverseOrder(getComparator()));
                // write the topk to the reducer
                for (Candidate candidate : list) {
                    candidate.write(similarityFile);
                }
            }
        }
    }

    @Override
    public void cleanup(Context context) {
        TestGenericJob.addMeasuresCompared(context, Document.getSimilarityFunction().getComparisons());
        TestGenericJob.addGetDocumentsTime(context, AnnIndex.getGetDocumentsTime(), AnnIndex.getGetDocumentsCount());
        TestGenericJob.addGetDocCodepoints(context, index.countDocCodepoints);
        TestGenericJob.addGetDocComparedCodepoints(context, index.countComparedDocCodepoints);
        TestGenericJob.addFingerprintTime(context, AnnIndex.getGetFingerprintTime(), AnnIndex.getGetFingerprintCount());
        TestGenericJob.addSimilarityFunction(context, Document.getSimilarityFunction().getComparisonsTime(),
                Document.getSimilarityFunction().getComparisons());
        similarityFile.closeWrite();
    }

    public Configuration getConf() {
        return conf;
    }
}
