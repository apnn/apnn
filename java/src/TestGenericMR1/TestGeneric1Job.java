package TestGenericMR1;

import BruteForce.AnnBruteForce;
import SimilarityFunction.CosineSimilarityTFIDF;
import static TestGenericMR.TestGenericJob.ANNINDEXCLASS;
import static TestGenericMR.TestGenericJob.SIMILARITYFUNCTIONCLASS;
import static TestGenericMR.TestGenericJob.getScanTopK;
import static TestGenericMR.TestGenericJob.getTopK;
import static TestGenericMR.TestGenericJob.setAnnIndex;
import static TestGenericMR.TestGenericJob.setSimilarityFunction;
import io.github.htools.lib.Log;
import io.github.htools.hadoop.Conf;
import io.github.htools.hadoop.Job;
import io.github.htools.hadoop.io.NullInputFormat;
import static io.github.htools.lib.PrintTools.sprintf;
import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

/**
 * Computes the cosine similarity between all suspicious and source documents of
 * the PAN11 collection. This class retrieves the k-most similar source
 * documents given a suspicious document, optionally set k (default=100) as the
 * maximum number of most similar source documents to retrieve and set the
 * similarity function (default=CosineSimilarity) that is used to score the
 * similarity between two documents.
 *
 * The default mapper does a brute force comparison suspicious document with all
 * source documents.
 *
 * The default reducer keeps only the k-most similar source document per
 * suspicious document and stores the result in a SimilarityFile. Override the
 * configured Mapper and Reducer to change the default operation.
 *
 * parameters:
 *
 * sources: HDFS path containing the PAN11 source documents wrapped in
 * ArchiveFiles (e.g. .tar.lz4) suspicious: HDFS path containing the PAN11
 * suspicious documents wrapped in ArchiveFiles (e.g. .tar.lz4) output: the
 * resulting k-most similar source documents per suspicious document are written
 * to a file with this name in SimilarityFile format
 *
 * @author Jeroen
 */
public class TestGeneric1Job extends Job {

    private static final Log log = new Log(TestGeneric1Job.class);

    public TestGeneric1Job(Conf conf, String collection, String query, String output, String vocabulary) throws IOException, ClassNotFoundException {
        super(conf, collection, query, output, vocabulary);
        conf.set("input", collection);
        conf.set("output", output);
        conf.set("query", query);
        conf.set("vocabulary", vocabulary);
        setInputFormatClass(NullInputFormat.class);
        setMapperClass(TestGenericMap1.class);
        setOutputFormatClass(NullOutputFormat.class);
        setNumReduceTasks(0);

        // By default use CosineSimilarity to score the similarity between documents
        if (!conf.containsKey(SIMILARITYFUNCTIONCLASS)) {
            setSimilarityFunction(getConfiguration(), CosineSimilarityTFIDF.class);
        }
        if (!conf.containsKey(ANNINDEXCLASS)) {
            setAnnIndex(getConfiguration(), AnnBruteForce.class);
        }
    }
    
    public static Conf tweakConf(Conf conf) {
        
        conf.raiseMapMemoryMB(4096);
        conf.setTaskTimeout(30000000);
        conf.setMapSpeculativeExecution(false);
        conf.setSortMB(100);
        return conf;
    }

    private String[] getParameters(Configuration conf, String... args) {
        ArrayList<String> parameters = new ArrayList();
        for (String a : args) {
            parameters.add(a);
        }
        parameters.add(sprintf("ann=%s", conf.get(ANNINDEXCLASS)));
        parameters.add(sprintf("topk=%s", getTopK(conf)));
        parameters.add(sprintf("scantopk=%s", getScanTopK(conf)));
        addParameters(conf, parameters);
        return parameters.toArray(new String[0]);
    }
    
    protected void addParameters(Configuration conf, ArrayList<String> parameters) {
    }
    
    public void useDocumentContent() {
        setMapperClass(TestGenericMapContent.class);
    }

    public void useDocumentTFIDF() {
        setMapperClass(TestGenericMapTFIDF.class);
    }

    public void useDocumentContentTrec() {
        setMapperClass(TestGenericMapContentTrec.class);
    }
    
    public void submitJob() throws ClassNotFoundException, InterruptedException, IOException {
        this.setJobName(getParameters(getConfiguration(), 
                conf.get("source"), conf.get("query"), conf.get("output")));
        if (this.getConfiguration().getBoolean("noninteractive", false)) {
            this.submit();
        } else {
            this.waitForCompletion(true);
        }
    }
    

    
}
