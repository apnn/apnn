package Canopy;

import SimilarityFile.SimilarityWritable;
import SimilarityFunction.CosineSimilarityTFIDF;
import SimilarityFunction.SimilarityFunction;
import TestGeneric.Candidate;
import TestGenericMR.SourceQueryPairInputFormat;
import TestGenericMR.TestGenericJob.COUNTERS;
import static TestGenericMR.TestGenericJob.SIMILARITYFUNCTIONCLASS;
import static TestGenericMR.TestGenericJob.getScanTopK;
import static TestGenericMR.TestGenericJob.getTopK;
import static TestGenericMR.TestGenericJob.setSimilarityFunction;
import io.github.htools.lib.Log;
import io.github.htools.hadoop.Conf;
import io.github.htools.hadoop.InputFormat;
import io.github.htools.hadoop.Job;
import io.github.htools.io.Datafile;
import io.github.htools.io.HDFSPath;
import io.github.htools.lib.ClassTools;
import static io.github.htools.lib.PrintTools.sprintf;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Comparator;
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
public class CanopyGenericJob extends Job {

    private static final Log log = new Log(CanopyGenericJob.class);
    public static final String ANNINDEXCLASS = CanopyGenericJob.class.getCanonicalName() + ".annindexclass";
    private static final String TERMSSIZE = CanopyJob.class.getCanonicalName() + ".termssize";
    private static final String T1 = CanopyJob.class.getCanonicalName() + ".t1";
    private static final String T2 = CanopyJob.class.getCanonicalName() + ".t2";
    String source;
    String query;
    String output;
    String vocabulary;

    public CanopyGenericJob(Conf conf, String source, String query, String output, String vocabulary) throws IOException {
        super(tweakConf(conf));
        this.source = source;
        this.query = query;
        this.output = output;
        this.vocabulary = vocabulary;
        setupInputFormat(this, source, query);
        setMapperClass(CanopyMap.class);
        setMapOutputKeyClass(Text.class);
        setMapOutputValueClass(SimilarityWritable.class);
        setNumReduceTasks(1);
        setReducerClass(CanopyReduce.class);
        setOutputFormatClass(NullOutputFormat.class);

        // By default use CosineSimilarity to score the similarity between documents
        if (!conf.containsKey(SIMILARITYFUNCTIONCLASS)) {
            setSimilarityFunction(getConfiguration(), CosineSimilarityTFIDF.class);
        }
    }

    public void submitJob() throws ClassNotFoundException, InterruptedException, IOException {
        this.setJobName(getParameters(getConfiguration(), source, query, output));
        if (this.getConfiguration().getBoolean("noninteractive", false)) {
            this.submit();
        } else {
            this.waitForCompletion(true);
        }
    }

    public static Conf tweakConf(Conf conf) {
        conf.setMapMemoryMB(8192);
        conf.setTaskTimeout(3000000);
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
        HDFSPath sourcePath = new HDFSPath(job.getConfiguration(), sources);
        HDFSPath queryPath = new HDFSPath(job.getConfiguration(), queries);
        ArrayList<Datafile> sourceFiles = sourcePath.getFiles();
        ArrayList<Datafile> queryFiles = queryPath.getFiles();

        // add all possible combinations of a sourceFile with a SuspiciousFile
        // to the input that is mapped.
        if (InputFormat.getSplitSize(job) >= Long.MAX_VALUE) {
            for (Datafile sourceFile : sourceFiles) {
                for (Datafile queryFile : queryFiles) {
                    SourceQueryPairInputFormat.add(job, sourceFile.getCanonicalPath(),
                            queryFile.getCanonicalPath());
                }
                SourceQueryPairInputFormat.add(job, sourceFile.getCanonicalPath(), null);
            }
        } else {
            for (Datafile sourceFile : InputFormat.split(job, sourceFiles)) {
                for (Datafile suspiciousFile : InputFormat.split(job, queryFiles)) {
                    SourceQueryPairInputFormat.add(job, sourceFile, suspiciousFile);
                }
                SourceQueryPairInputFormat.add(job, sourceFile.getCanonicalPath(), null);
            }
        }
    }

    public void useDocumentTFIDF() {
        setMapperClass(CanopyMapTFIDF.class);
    }

    public void useDocumentTFIDF2() {
        setMapperClass(CanopyMapTFIDF2.class);
    }

    /**
     * Configure the implementation of AnnIndex to use
     *
     * @param job
     * @param clazz
     */
    public static void setAnnIndex(Configuration conf, Class<? extends AnnCanopy> clazz) {
        conf.set(ANNINDEXCLASS, clazz.getCanonicalName());
    }

    /**
     * @param conf
     * @return an instance of the configured implementation to use as AnnIndex,
     * using the configured similarityFunction
     * @throws ClassNotFoundException
     */
    public static AnnCanopy getAnnIndex(
            Comparator<SimilarityWritable> comparator,
            Configuration conf) {
        String clazzname = conf.get(ANNINDEXCLASS);
        log.info("getAnnIndex %s %s %s", clazzname, comparator, conf);
        try {
            Class clazz = ClassTools.toClass(clazzname);

            Constructor<AnnCanopy> constructor = ClassTools.getAssignableConstructor(clazz, AnnCanopy.class, Comparator.class, Configuration.class);

            return ClassTools.construct(constructor, comparator, conf);
        } catch (ClassNotFoundException ex) {
            log.fatalexception(ex, "getAnnIndex %s", clazzname);
            return null;
        }
    }

    public static void setT1(Configuration conf, double t1) {
        conf.setDouble(T1, t1);
    }

    public static double getT1(Configuration conf) {
        return conf.getDouble(T1, 0.9);
    }

    public static void setT2(Configuration conf, double t2) {
        conf.setDouble(T2, t2);
    }

    public static double getT2(Configuration conf) {
        return conf.getDouble(T2, getT1(conf));
    }

    public static void setTermsSize(Configuration conf, int termssize) {
        conf.setInt(TERMSSIZE, termssize);
    }

    public static int getTermsSize(Configuration conf) {
        return conf.getInt(TERMSSIZE, 20);
    }

}
