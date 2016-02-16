package TestGenericMR;

import BruteForce.AnnBruteForce;
import SimilarityFile.SimilarityWritable;
import SimilarityFunction.CosineSimilarityTFIDF;
import SimilarityFunction.DotProduct;
import SimilarityFunction.SimilarityFunction;
import TestGeneric.AnnIndex;
import TestGeneric.Candidate;
import TestGeneric.ContentExtractor;
import TestGenericNYT.TestGenericNYTMapTFIDF2;
import TestGenericRobust.TestGenericFDMMapTFIDF;
import io.github.htools.hadoop.Conf;
import io.github.htools.hadoop.InputFormat;
import io.github.htools.hadoop.Job;
import io.github.htools.io.Datafile;
import io.github.htools.io.HDFSPath;
import io.github.htools.lib.ClassTools;
import io.github.htools.lib.Log;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Comparator;

import static io.github.htools.lib.PrintTools.sprintf;

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
public class TestGenericJob extends Job {

    private static final Log log = new Log(TestGenericJob.class);
    public static final String ANNINDEXCLASS = TestGenericJob.class.getCanonicalName() + ".annindexclass";
    public static final String SIMILARITYFUNCTIONCLASS = TestGenericJob.class.getCanonicalName() + ".similarityfunctionclass";
    public static final String CONTENTEXTRACTORCLASS = TestGenericJob.class.getCanonicalName() + ".contentextractor";
    String source;
    String query;
    String output;
    String vocabulary;

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

    public TestGenericJob(Conf conf, String source, String query, String output, String vocabulary) throws IOException {
        super(tweakConf(conf));
        this.source = source;
        this.query = query;
        this.output = output;
        this.vocabulary = vocabulary;
        setupInputFormat(source, query);
        setMapperClass(TestGenericMapTerms.class);
        setMapOutputKeyClass(Text.class);
        setMapOutputValueClass(Candidate.class);
        setNumReduceTasks(1);
        setReducerClass(TestGenericReduce.class);
        setOutputFormatClass(NullOutputFormat.class);

        // By default use CosineSimilarity to score the similarity between documents
        if (!conf.containsKey(SIMILARITYFUNCTIONCLASS)) {
            setSimilarityFunction(getConfiguration(), CosineSimilarityTFIDF.class);
        }
        if (!conf.containsKey(ANNINDEXCLASS)) {
            setAnnIndex(getConfiguration(), AnnBruteForce.class);
        }
        if (conf.containsKey("topk"))
            setTopK(conf, 100);
        // k-most similar documents to retrieve
        //this.setTopK(100);
        // tokenizer to use
    }

    public void submitJob() throws ClassNotFoundException, InterruptedException, IOException {
        this.setJobName(getParameters(getConfiguration(), source, query, output));
        if (this.getConfiguration().getBoolean("noninteractive", false)) {
            this.submit();
        } else {
            this.waitForCompletion(true);
        }
    }

    public void useDocumentTFIDF() {
        setMapperClass(TestGenericMapTFIDF.class);
    }

    public void useDocumentNYTTFIDF2() {
        TestGenericJob.setSimilarityFunction(getConfiguration(), DotProduct.class);
        setMapperClass(TestGenericNYTMapTFIDF2.class);
    }

    public void useDocumentFDMTFIDF() {
        setMapperClass(TestGenericFDMMapTFIDF.class);
    }

    public static Conf tweakConf(Conf conf) {
        conf.setMapMemoryMB(8192);
        conf.setTaskTimeout(30000000);
        conf.setMapSpeculativeExecution(false);
        conf.setSortMB(1000);
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

    protected void setupInputFormat(String sources, String queries) throws IOException {
        setupInputFormat(this, sources, queries);
    }
    
    /**
     * Uses a custom InputFormat in which the key-value pairs are one source
     * file combined with one suspicious file, thus trying all combinations.
     *
     * @param job
     * @throws IOException
     */
    public static void setupInputFormat(Job job, String sources, String queries) throws IOException {
        job.setInputFormatClass(SourceQueryPairInputFormat.class);

        // get lists of files under the paths of sources and suspicious on HDFS
        HDFSPath sourcepath = new HDFSPath(job.getConfiguration(), sources);
        HDFSPath querypath = new HDFSPath(job.getConfiguration(), queries);
        ArrayList<Datafile> sourceFiles = sourcepath.getFiles();
        ArrayList<Datafile> queryFiles = querypath.getFiles();

        // add all possible combinations of a sourceFile with a SuspiciousFile
        // to the input that is mapped.
        if (InputFormat.getSplitSize(job) >= Long.MAX_VALUE) {
            for (Datafile sourceFile : sourceFiles) {
                for (Datafile suspiciousFile : queryFiles) {
                    SourceQueryPairInputFormat.add(job, sourceFile.getCanonicalPath(),
                            suspiciousFile.getCanonicalPath());
                }
            }
        } else {
            for (Datafile sourceFile : InputFormat.split(job, sourceFiles)) {
                for (Datafile queryFile : InputFormat.split(job, queryFiles)) {
                    SourceQueryPairInputFormat.add(job, sourceFile, queryFile);
                }
            }

        }
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

    public static void setScanTopK(Configuration conf, int k) {
        conf.setInt("scantopk", k);
    }

    public static void setTopK(Configuration conf, int k) {
        conf.setInt("topk", k);
    }

    /**
     * Configure the implementation of SimilarityFunction to use
     *
     * @param conf
     * @param clazz
     */
    public static void setSimilarityFunction(Configuration conf, Class<? extends SimilarityFunction> clazz) {
        conf.set(SIMILARITYFUNCTIONCLASS, clazz.getCanonicalName());
    }

    /**
     * @param conf
     * @return an instance of the configured implementation of
     * SimilarityFunction, exits with a fatal when not properly configured
     */
    public static SimilarityFunction getSimilarityFunction(Configuration conf) {
        String clazzname = conf.get(SIMILARITYFUNCTIONCLASS);
        try {
            Class clazz = ClassTools.toClass(clazzname);
            Datafile vocabulary = new Datafile(conf, conf.get("vocabulary"));
            Constructor<SimilarityFunction> constructor
                    = ClassTools.getAssignableConstructor(clazz, SimilarityFunction.class, Datafile.class);
            SimilarityFunction function = ClassTools.construct(constructor, vocabulary);
            return function;
        } catch (ClassNotFoundException ex) {
            log.fatalexception(ex, "getSimilarityFunction %s", clazzname);
            return null;
        }
    }

    /**
     * Configure the implementation of SimilarityFunction to use
     *
     * @param conf
     * @param clazz
     */
    public static void setContentExtractor(Configuration conf, Class<? extends ContentExtractor> clazz) {
        conf.set(CONTENTEXTRACTORCLASS, clazz.getCanonicalName());
    }

    /**
     * @param conf
     * @return an instance of the configured implementation of
     * SimilarityFunction, exits with a fatal when not properly configured
     */
    public static ContentExtractor getContentExtractor(Configuration conf) {
        String clazzname = conf.get(CONTENTEXTRACTORCLASS);
        if (clazzname == null)
            return null;
        try {
            Class clazz = ClassTools.toClass(clazzname);
            Constructor<ContentExtractor> constructor
                    = ClassTools.getAssignableConstructor(clazz, ContentExtractor.class);
            ContentExtractor function = ClassTools.construct(constructor);
            return function;
        } catch (ClassNotFoundException ex) {
            log.fatalexception(ex, "getContentExtractor %s", clazzname);
            return null;
        }
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

    /**
     * Configure the implementation of AnnIndex to use
     *
     * @param conf
     * @param clazz
     */
    public static void setAnnIndex(Configuration conf, Class<? extends AnnIndex> clazz) {
        conf.set(ANNINDEXCLASS, clazz.getCanonicalName());
    }

    /**
     * @param conf
     * @return an instance of the configured implementation to use as AnnIndex,
     * using the configured similarityFunction
     * @throws ClassNotFoundException
     */
    public static AnnIndex getAnnIndex(
            Comparator<SimilarityWritable> comparator,
            Configuration conf) {
        String clazzname = conf.get(ANNINDEXCLASS);
        log.info("getAnnIndex %s %s", clazzname, comparator);
        try {
            Class clazz = ClassTools.toClass(clazzname);

            Constructor<AnnIndex> constructor = ClassTools.getAssignableConstructor(clazz, AnnIndex.class, Comparator.class, Configuration.class);

            return ClassTools.construct(constructor, comparator, conf);
        } catch (ClassNotFoundException ex) {
            log.fatalexception(ex, "getAnnIndex %s", clazzname);
            return null;
        }
    }

}
