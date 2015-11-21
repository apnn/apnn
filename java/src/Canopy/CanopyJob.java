package Canopy;

import SimilarityFunction.CosineSimilarityTFIDF;
import TestGenericMR.TestGenericJob;
import TestGenericMR1.TestGeneric1Job;
import io.github.htools.lib.Log;
import io.github.htools.hadoop.Conf;
import static io.github.htools.lib.PrintTools.sprintf;
import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.conf.Configuration;

/**
 * Computes the cosine similarity between all suspicious and source documents of
 * the PAN11 collection. The k (default=100) highest cosine similarities per
 * suspicious document are stored.
 *
 * parameters:
 *
 * sourcepath: HDFS path containing the PAN11 source documents wrapped in
 * ArchiveFiles (e.g. .tar.lz4) suspiciouspath: HDFS path containing the PAN11
 * suspicious documents wrapped in ArchiveFiles (e.g. .tar.lz4) output: the
 * resulting k-most similar source documents per suspicious document are written
 * to a file with this name in SimilarityFile format
 *
 * @author Jeroen
 */
public class CanopyJob extends TestGeneric1Job {

    private static final Log log = new Log(CanopyJob.class);
    private static final String TERMSSIZE = CanopyJob.class.getCanonicalName() + ".termssize";
    private static final String T1 = CanopyJob.class.getCanonicalName() + ".t1";
    private static final String T2 = CanopyJob.class.getCanonicalName() + ".t2";

    public static void main(String[] args) throws Exception {

        Conf conf = new Conf(args, "source query output vocabulary -k [termssize] -t [t1] -s [t2]");
        conf.setTaskTimeout(1000000);
        conf.setMapMemoryMB(8096);
        TestGenericJob.setAnnIndex(conf, AnnCanopyCosine.class);
        TestGenericJob.setSimilarityFunction(conf, CosineSimilarityTFIDF.class);
        if (conf.containsKey("t1")) {
            setT1(conf, conf.getDouble("t1", 0));
        }
        if (conf.containsKey("t2")) {
            setT1(conf, conf.getDouble("t2", 0));
        }
        setTermsSize(conf, conf.getInt("termssize", getTermsSize(conf)));

        CanopyJob job = new CanopyJob(conf,
                conf.get("source"),
                conf.get("query"),
                conf.get("output"),
                conf.get("vocabulary"));

        job.useDocumentTFIDF();

        // configuration example (used as default):
        job.waitForCompletion(true);
    }

    
    protected void addParameters(Configuration conf, ArrayList<String> parameters) {
        parameters.add(sprintf("k=%d", getTermsSize(conf)));
        parameters.add(sprintf("t1=%.2f", getT1(conf)));
        parameters.add(sprintf("t2=%.2f", getT2(conf)));
    }
    
    public static void setTermsSize(Configuration conf, int termssize) {
        conf.setInt(TERMSSIZE, termssize);
    }

    public static int getTermsSize(Configuration conf) {
        return conf.getInt(TERMSSIZE, 20);
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

    public CanopyJob(Conf conf, String collection, String query, String output, String vocabulary) throws IOException, ClassNotFoundException {
        super(conf, collection, query, output, vocabulary);
    }
}
