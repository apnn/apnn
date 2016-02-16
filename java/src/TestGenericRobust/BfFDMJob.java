package TestGenericRobust;

import TestGenericMR.*;
import TestGeneric.Document;
import io.github.htools.lib.Log;
import io.github.htools.hadoop.Conf;
import io.github.htools.hadoop.Job;
import io.github.htools.hadoop.io.StringInputFormat;
import io.github.htools.io.Datafile;
import io.github.htools.io.HDFSPath;
import static io.github.htools.lib.PrintTools.sprintf;
import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
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
public class BfFDMJob extends Job {

    private static final Log log = new Log(BfFDMJob.class);

    public static enum COUNTERS {

        MEASURESCOMPARED,
        DOCCODEPOINTS,
        DOCCOMPAREDCODEPOINTS,
        GETDOCUMENTSTIME,
        GETDOCUMENTSCOUNT,
        FINGERPRINTTIME,
        FINGERPRINTCOUNT,
        COSINESIMILARITYTIME,
        COSINESIMILARITYCOUNT
    }

    public BfFDMJob(Conf conf, String source, String queries, String output, String vocabulary) throws IOException {
        super(tweakConf(conf));
        setupInputFormat(this, source);
        conf.set("source", source);
        conf.set("output", output);
        conf.set("fdmvoc", vocabulary);
        conf.set("queries", queries);
        conf.set("fdmvoc", vocabulary);
        setMapperClass(BfFDMMap.class);
        setMapOutputKeyClass(Text.class);
        setMapOutputValueClass(FDMDoc.class);
        setNumReduceTasks(250);
        setReducerClass(BfFDMReduce.class);
        setOutputFormatClass(NullOutputFormat.class);

        // k-most similar documents to retrieve
        //this.setTopK(100);
        // tokenizer to use
    }

    public void submitJob() throws ClassNotFoundException, InterruptedException, IOException {
        this.setJobName(getParameters(getConfiguration()));
        if (this.getConfiguration().getBoolean("noninteractive", false)) {
            this.submit();
        } else {
            this.waitForCompletion(true);
        }
    }

    public static Conf tweakConf(Conf conf) {
        conf.setMapMemoryMB(8192);
        conf.setTaskTimeout(30000000);
        conf.setMapSpeculativeExecution(false);
        conf.setSortMB(100);
        return conf;
    }

    private String[] getParameters(Configuration conf, String... args) {
        ArrayList<String> parameters = new ArrayList();

        parameters.add(sprintf("source=%s", conf.get("source")));
        parameters.add(sprintf("queries=%s", conf.get("queries")));
        parameters.add(sprintf("output=%s", conf.get("output")));
        parameters.add(sprintf("fdmvoc=%s", conf.get("fdmvoc")));
        parameters.add(sprintf("topk=%s", getTopK(conf)));
        parameters.add(sprintf("scantopk=%s", getScanTopK(conf)));
        addParameters(conf, parameters);
        return parameters.toArray(new String[0]);
    }

    protected void addParameters(Configuration conf, ArrayList<String> parameters) {
    }

    /**
     * Uses a custom InputFormat in which the key-value pairs are one source
     * file combined with one suspicious file, thus trying all combinations.
     *
     * @param job
     * @throws IOException
     */
    public static void setupInputFormat(Job job, String sources) throws IOException {
        job.setInputFormatClass(StringInputFormat.class);

        // get lists of files under the paths of sources and suspicious on HDFS
        HDFSPath sourcepath = new HDFSPath(job.getConfiguration(), sources);
        ArrayList<Datafile> sourceFiles = sourcepath.getFiles();

        for (Datafile sourceFile : sourceFiles) {
            StringInputFormat.add(job, sourceFile.getCanonicalPath());
        }
    }

    public static ArrayList<FDMQuery> setQueries(Configuration conf, DocumentReader documentreader) throws IOException {
        // iterate over all suspicious documents
        ArrayList<FDMQuery> queries = new ArrayList();
        Datafile queryDF = new Datafile(conf, conf.get("queries"));
        for (Document doc : documentreader.iterableDocuments(queryDF)) {
            FDMQuery query = new FDMQuery(doc.docid, doc.getTerms());
            queries.add(query);
        }
        return queries;
    }    
    
    public static ArrayList<Document> getQuerydocs(Configuration conf, DocumentReader documentreader) throws IOException {
        Datafile queryDF = new Datafile(conf, conf.get("queries"));
        return documentreader.readDocuments(queryDF);
    }    
    
    /**
     * @param conf
     * @return configured number of most similar matches to retrieve,
     * default=100
     */
    public static int getTopK(Configuration conf) {
        return conf.getInt("topk", 100);
    }

    /**
     * @param conf
     * @return configured number of most similar matches to scan, default=topk
     */
    public static int getScanTopK(Configuration conf) {
        return conf.getInt("scantopk", getTopK(conf));
    }

    public static void addMeasuresCompared(TaskInputOutputContext conf, int count) {
        conf.getCounter(COUNTERS.MEASURESCOMPARED).increment(count);
    }

    public static void addGetDocumentsTime(TaskInputOutputContext conf, long time, long count) {
        conf.getCounter(COUNTERS.GETDOCUMENTSTIME).increment(time);
        conf.getCounter(COUNTERS.GETDOCUMENTSCOUNT).increment(count);
    }

    public static void addGetDocCodepoints(TaskInputOutputContext conf, long count) {
        conf.getCounter(COUNTERS.DOCCODEPOINTS).increment(count);
    }

    public static void addGetDocComparedCodepoints(TaskInputOutputContext conf, long count) {
        conf.getCounter(COUNTERS.DOCCOMPAREDCODEPOINTS).increment(count);
    }

    public static void addFingerprintTime(TaskInputOutputContext conf, long time, long count) {
        conf.getCounter(COUNTERS.FINGERPRINTTIME).increment(time);
        conf.getCounter(COUNTERS.FINGERPRINTCOUNT).increment(count);
    }

    public static void addSimilarityFunction(TaskInputOutputContext conf, long time, long count) {
        conf.getCounter(COUNTERS.COSINESIMILARITYTIME).increment(time);
        conf.getCounter(COUNTERS.COSINESIMILARITYCOUNT).increment(count);
    }


}
