package TestGenericMR;

import BruteForce.AnnBruteForce;
import SimilarityFile.SimilarityWritable;
import SimilarityFunction.CosineSimilarityTFIDF;
import SimilarityFunction.SimilarityFunction;
import TestGeneric.AnnIndex;
import TestGeneric.Candidate;
import TestGeneric.Tokenizer;
import TestGeneric.TokenizerRemoveStopwords;
import io.github.htools.extract.AbstractTokenizer;
import io.github.htools.lib.Log;
import io.github.htools.hadoop.Conf;
import io.github.htools.hadoop.Job;
import io.github.htools.io.Datafile;
import io.github.htools.io.HDFSPath;
import io.github.htools.lib.ArrayTools;
import io.github.htools.lib.ClassTools;
import static io.github.htools.lib.PrintTools.sprintf;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Comparator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
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
public class TestGenericJob extends Job {

    private static final Log log = new Log(TestGenericJob.class);
    public static final String ANNINDEXCLASS = TestGenericJob.class.getCanonicalName() + ".AnnIndexClass";
    public static final String TOKENIZERCLASS = TestGenericJob.class.getCanonicalName() + ".tokenizer";
    public static final String SIMILARITYFUNCTIONCLASS = TestGenericJob.class.getCanonicalName() + ".similarityfunctionclass";
    Conf conf;
    String sources;
    String suspicious;
    String output;

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

    public TestGenericJob(Conf conf, String sources, String suspicious, String output) throws IOException {
        super(conf);
        this.sources = sources;
        this.suspicious = suspicious;
        this.output = output;
        this.conf = conf;
        conf.setMapMemoryMB(4096);
        conf.setTaskTimeout(30000000);
        conf.setMapSpeculativeExecution(false);
        conf.setSortMB(1000);
        setupInputFormat(this, sources, suspicious);
        setMapperClass(TestGenericMap.class);
        setMapOutputKeyClass(IntWritable.class);
        setMapOutputValueClass(Candidate.class);
        setNumReduceTasks(1);
        setReducerClass(TestGenericReduce.class);
        setOutputFormatClass(NullOutputFormat.class);

        // By default use CosineSimilarity to score the similarity between documents
        setSimilarityFunction(CosineSimilarityTFIDF.class);
        setAnnIndex(AnnBruteForce.class);
        // k-most similar documents to retrieve
        //this.setTopK(100);
        // tokenizer to use
        this.setTokenizer(TokenizerRemoveStopwords.class);
    }
    
    public void submit() throws IOException, InterruptedException, ClassNotFoundException {
        this.setJobName(getParameters(getConfiguration(), sources, suspicious, output));
        super.submit();
    }
    
    private String[] getParameters(Configuration conf, String ... args) {
        ArrayList<String> parameters = new ArrayList();
        for (String a : args)
            parameters.add(a);
        parameters.add(sprintf("ann=%s", conf.get(ANNINDEXCLASS)));
        parameters.add(sprintf("voc=%s", conf.get("vocabulary")));
        parameters.add(sprintf("topk=%s", getTopK(conf)));
        parameters.add(sprintf("scantopk=%s", getScanTopK(conf)));
        addParameters(conf, parameters);
        return parameters.toArray(new String[0]);
    }
    
    protected void addParameters(Configuration conf, ArrayList<String> parameters) { }

    /**
     * Uses a custom InputFormat in which the key-value pairs are one source
     * file combined with one suspicious file, thus trying all combinations.
     *
     * @param job
     * @throws IOException
     */
    public static void setupInputFormat(Job job, String sources, String suspicious) throws IOException {
        job.setInputFormatClass(StringPairInputFormat.class);

        // get lists of files under the paths of sources and suspicious on HDFS
        HDFSPath sourcepath = new HDFSPath(job.getConfiguration(), sources);
        HDFSPath suspiciouspath = new HDFSPath(job.getConfiguration(), suspicious);
        ArrayList<String> sourceFiles = sourcepath.getFilepathnames();
        ArrayList<String> suspiciousFiles = suspiciouspath.getFilepathnames();

        // add all possible combinations of a sourceFile with a SuspiciousFile
        // to the input that is mapped.
        for (String sourceFile : sourceFiles) {
            for (String suspiciousFile : suspiciousFiles) {
                StringPairInputFormat.add(job, sourceFile, suspiciousFile);
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
     * @return configured number of most similar matches to scan,
     * default=topk
     */
    public static int getScanTopK(Configuration conf) {
        return conf.getInt("scantopk", getTopK(conf));
    }
    
    /**
     * Configure the implementation of SimilarityFunction to use
     *
     * @param job
     * @param clazz
     */
    public void setSimilarityFunction(Class<? extends SimilarityFunction> clazz) {
        getConfiguration().set(SIMILARITYFUNCTIONCLASS, clazz.getCanonicalName());
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

            if (conf.get("vocabulary") != null) {
                Datafile vocabulary = new Datafile(conf, conf.get("vocabulary"));
                Constructor<SimilarityFunction> constructor
                        = ClassTools.getAssignableConstructor(clazz, SimilarityFunction.class, Datafile.class);
                SimilarityFunction function = ClassTools.construct(constructor, vocabulary);
                return function;
            } else {
                Constructor<SimilarityFunction> constructor
                        = ClassTools.getAssignableConstructor(clazz, SimilarityFunction.class);
                SimilarityFunction function = ClassTools.construct(constructor);
                return function;
            }
        } catch (ClassNotFoundException ex) {
            log.fatalexception(ex, "getSimilarityFunction %s", clazzname);
            return null;
        }
    }

    /**
     * Configure the implementation of DefaultTokenizer to use
     *
     * @param job
     * @param clazz
     */
    public void setTokenizer(Class<? extends AbstractTokenizer> clazz) {
        getConfiguration().set(TOKENIZERCLASS, clazz.getCanonicalName());
    }

    /**
     * @param conf
     * @return the configured tokenizer or null if not configured. The tokenizer
     * must extend DefaultTokenizer.
     */
    public static Class<? extends Tokenizer> getTokenizerClass(Configuration conf) {
        String clazzname = conf.get(TOKENIZERCLASS, TokenizerRemoveStopwords.class.getCanonicalName());
        if (clazzname != null) {
            Class clazz = ClassTools.toClass(clazzname);
            if (!Tokenizer.class.isAssignableFrom(clazz)) {
                log.fatal("configured tokenizer must be assignable from Tokenizer ( %s )", clazzname);
            }
            return clazz;
        }
        return null;
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
     * @param job
     * @param clazz
     */
    public void setAnnIndex(Class<? extends AnnIndex> clazz) {
        getConfiguration().set(ANNINDEXCLASS, clazz.getCanonicalName());
    }

    /**
     * @param conf
     * @return an instance of the configured implementation to use as AnnIndex,
     * using the configured similarityFunction
     * @throws ClassNotFoundException
     */
    public static AnnIndex getAnnIndex(SimilarityFunction function, 
                                       Comparator<SimilarityWritable> comparator, 
                                       Configuration conf) {
        String clazzname = conf.get(ANNINDEXCLASS);
        log.info("getAnnIndex %s", clazzname);
        try {
            Class clazz = ClassTools.toClass(clazzname);

            Constructor<AnnIndex> constructor = ClassTools.getAssignableConstructor(clazz, AnnIndex.class, SimilarityFunction.class, Comparator.class, Configuration.class);

            return ClassTools.construct(constructor, function, comparator, conf);
        } catch (ClassNotFoundException ex) {
            log.fatalexception(ex, "getAnnIndex %s", clazzname);
            return null;
        }
    }

}
