package TestGenericMR;

import SimilarityFile.SimilarityWritable;
import SimilarityFunction.CosineSimilarity;
import SimilarityFunction.SimilarityFunction;
import io.github.htools.extract.AbstractTokenizer;
import io.github.htools.extract.DefaultTokenizer;
import io.github.htools.lib.Log;
import io.github.htools.hadoop.Conf;
import io.github.htools.hadoop.Job;
import io.github.htools.io.HDFSPath;
import io.github.htools.lib.ClassTools;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
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
    public static final String TOKENIZERCLASS = TestGenericJob.class.getCanonicalName() + ".tokenizer";
    public static final String SIMILARITYFUNCTIONCLASS = TestGenericJob.class.getCanonicalName() + ".similarityfunctionclass";
    public static final String TOPK = TestGenericJob.class.getCanonicalName() + ".topk";
    Conf conf;

    public static enum COUNTERS {

        COMPARISON
    }

    public TestGenericJob(Conf conf, String sources, String suspicious, String outFile) throws IOException {
        super(conf, sources, suspicious, outFile);
        this.conf = conf;
        conf.setMapMemoryMB(4096);
        conf.setTaskTimeout(30000000);
        conf.setMapSpeculativeExecution(false);
        conf.setSortMB(1000);
        setupInputFormat(sources, suspicious);
        setMapperClass(TestGenericMap.class);
        setMapOutputKeyClass(IntWritable.class);
        setMapOutputValueClass(SimilarityWritable.class);
        setNumReduceTasks(1);
        setReducerClass(TestGenericReduce.class);
        setOutputFormatClass(NullOutputFormat.class);

        // By default use CosineSimilarity to score the similarity between documents
        setSimilarityFunction(CosineSimilarity.class);
        // k-most similar documents to retrieve
        //this.setTopK(100);
        // tokenizer to use
        //this.setTokenizer(TokenizerRemoveStopwords.class);
    }

    /**
     * Uses a custom InputFormat in which the key-value pairs are one source
     * file combined with one suspicious file, thus trying all combinations.
     *
     * @param job
     * @throws IOException
     */
    void setupInputFormat(String sources, String suspicious) throws IOException {
        setInputFormatClass(StringPairInputFormat.class);

        // get lists of files under the paths of sources and suspicious on HDFS
        HDFSPath sourcepath = new HDFSPath(conf, sources);
        HDFSPath suspiciouspath = new HDFSPath(conf, suspicious);
        ArrayList<String> sourceFiles = sourcepath.getFilepathnames();
        ArrayList<String> suspiciousFiles = suspiciouspath.getFilepathnames();

        // add all possible combinations of a sourceFile with a SuspiciousFile
        // to the input that is mapped.
        for (String sourceFile : sourceFiles) {
            for (String suspiciousFile : suspiciousFiles) {
                StringPairInputFormat.add(this, sourceFile, suspiciousFile);
            }
        }
    }

    /**
     * Configure the maximum number of most similar documents to retrieve
     *
     * @param job
     * @param k
     */
    public void setTopK(int k) {
        getConfiguration().setInt(TOPK, k);
    }

    /**
     * @param conf
     * @return configured number of most similar matches to retrieve,
     * default=100
     */
    public static int getTopK(Configuration conf) {
        return conf.getInt(TOPK, 100);
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

            Constructor<SimilarityFunction> constructor
                    = ClassTools.getAssignableConstructor(clazz, SimilarityFunction.class);
            SimilarityFunction function = ClassTools.construct(constructor);
            return function;
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
    public static Class<? extends DefaultTokenizer> getTokenizerClass(Configuration conf) {
        String clazzname = conf.get(TOKENIZERCLASS);
        if (clazzname != null) {
            Class clazz = ClassTools.toClass(clazzname);
            if (!AbstractTokenizer.class.isAssignableFrom(clazz)) {
                log.fatal("configured tokenizer must be assignable from AbstractTokenizer ( %s )", clazzname);
            }
            return clazz;
        }
        return null;
    }
        
    public static void countComparison(TaskInputOutputContext conf, int count) {
        conf.getCounter(COUNTERS.COMPARISON).increment(count);
    }

}
