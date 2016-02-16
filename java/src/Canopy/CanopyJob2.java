package Canopy;

import SimilarityFunction.CosineSimilarityTFIDF;
import TestGenericMR.TestGenericJob;
import io.github.htools.hadoop.Conf;
import io.github.htools.lib.Log;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.util.ArrayList;

import static io.github.htools.lib.PrintTools.sprintf;

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
public class CanopyJob2 extends CanopyGenericJob {

    private static final Log log = new Log(CanopyJob2.class);

    public static void main(String[] args) throws Exception {

        Conf conf = new Conf(args, "source query output vocabulary -t [t1] --noninteractive");
        conf.setTaskTimeout(1000000);
        conf.setMapMemoryMB(8096);
        conf.setReduceMemoryMB(8096 * 4);
        CanopyGenericJob.setAnnIndex(conf, AnnCanopyCosine2.class);
        TestGenericJob.setSimilarityFunction(conf, CosineSimilarityTFIDF.class);
        if (conf.containsKey("t1")) {
            setT1(conf, conf.getDouble("t1", 0));
        }
        setTermsSize(conf, conf.getInt("termssize", getTermsSize(conf)));

        CanopyJob2 job = new CanopyJob2(conf,
                conf.get("source"),
                conf.get("query"),
                conf.get("output"),
                conf.get("vocabulary"));

        job.useDocumentTFIDF2();

        // configuration example (used as default):
        job.submitJob();
    }

    
    protected void addParameters(Configuration conf, ArrayList<String> parameters) {
        parameters.add(sprintf("t1=%.2f", getT1(conf)));
    }

    public CanopyJob2(Conf conf, String collection, String query, String output, String vocabulary) throws IOException, ClassNotFoundException {
        super(conf, collection, query, output, vocabulary);
    }
}
