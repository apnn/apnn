package TestGenericRobust;

import SimilarityFile.MeasureSimilarity;
import SimilarityFile.SimilarityFile;
import SimilarityFile.SimilarityWritable;
import TestGeneric.Candidate;
import TestGeneric.CandidateList;
import TestGenericMR.DocumentReaderTerms;
import static TestGenericRobust.BfFDMJob.setQueries;
import io.github.htools.hadoop.Conf;
import io.github.htools.hadoop.ContextTools;
import io.github.htools.lib.Log;
import io.github.htools.lib.Profiler;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * FDM is scored here as query likelihood like in the original, however Zhai's
 * Dirichlet smoothed LM notation is rank equivalent and faster.
 *
 * @author jeroen
 */
public class BfFDMReduce extends Reducer<Text, FDMDoc, NullWritable, NullWritable> {

    public static final Log log = new Log(BfFDMReduce.class);

    enum REDUCE {

        RETRIEVED,
        SCANNED,
        RETURNED,
        LM,
        LMCOUNT
    }
    Conf conf;
    Comparator<SimilarityWritable> comparator;
    // the number of most similar documents to keep, configurable as "topk".
    int resultSize;
    int scanSize;
    FDMParameters params;
    HashMap<String, FDMQuery> queries = new HashMap();

    @Override
    public void setup(Context context) throws IOException {
        conf = ContextTools.getConfiguration(context);
        resultSize = BfFDMJob.getTopK(conf);
        scanSize = resultSize;
        comparator = getComparator();
        params = new FDMParameters(conf);
        for (FDMQuery q : setQueries(conf, new DocumentReaderTerms())) {
            queries.put(q.id, q);
        }
        //log.info("createVoc %b", createVoc);
    }

    @Override
    public void reduce(Text key, Iterable<FDMDoc> values, Context context) throws IOException, InterruptedException {
        FDMQuery query = queries.get(key.toString());
        SimilarityFile similarityFile = new SimilarityFile(conf.getHDFSPath("output").getFile(key.toString()));
        similarityFile.openWrite();
        ArrayList<FDMDoc> docs = new ArrayList();

        for (FDMDoc value : values) {
            //log.info("add %s", value.id);
            docs.add(value.clone());
        }
        FDMVoc voc = new FDMVoc(conf, key.toString());
        for (FDMDoc doc : docs) {
            voc.addVoc(doc);
        }

        CandidateList candidates = new CandidateList(resultSize, comparator);
        for (FDMDoc doc : docs) {
            double lm = score(query, doc, voc);
            Candidate candidate = new Candidate();
            candidate.query = key.toString();
            candidate.indexSimilarity = lm;
            candidate.measureSimilarity = lm;
            candidate.source = doc.id;
            candidates.add(candidate);
        }
        for (Candidate candidate : candidates.sorted()) {
            candidate.write(similarityFile);
        }
        voc.save();
        similarityFile.closeWrite();
    }
    
    public void cleanup(Context context) {
        context.getCounter(REDUCE.LM).increment(Profiler.totalTimeMs("getlm"));
        context.getCounter(REDUCE.LMCOUNT).increment(Profiler.getCount("getlm"));
    }

    public double score(FDMQuery query, FDMDoc doc, FDMVoc voc) {
        return doc.getFDM(query, voc, params);
    }
    
    
    public void setScanSize(int scanSize) {
        this.scanSize = scanSize;
    }

    public Comparator<SimilarityWritable> getComparator() {
        return MeasureSimilarity.singleton;
    }
}
