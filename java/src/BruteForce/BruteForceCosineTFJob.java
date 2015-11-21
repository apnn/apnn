package BruteForce;

import TestGenericMR.TestGenericJob;
import io.github.htools.lib.Log;
import io.github.htools.hadoop.Conf;
import java.io.IOException;

/**
 * Computes the cosine similarity between all suspicious and source documents of
 * the PAN11 collection. The k (default=100) highest cosine similarities per
 * suspicious document are stored.
 *
 * parameters:
 *
 * sourcepath: HDFS path containing the PAN11 source documents wrapped in
 * ArchiveFiles (e.g. .tar.lz4) 
 * suspiciouspath: HDFS path containing the PAN11
 * suspicious documents wrapped in ArchiveFiles (e.g. .tar.lz4)
 * output: the resulting k-most similar source documents per suspicious document
 * are written to a file with this name in SimilarityFile format
 *
 * @author Jeroen
 */
public class BruteForceCosineTFJob {

    private static final Log log = new Log(BruteForceCosineTFJob.class);

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Conf conf = new Conf(args, "sourcepath suspiciouspath output vocabulary");
        conf.setTaskTimeout(1000000);
        TestGenericJob.setAnnIndex(conf, AnnBruteForceTF.class);
        
        TestGenericJob job = new TestGenericJob(conf,
                conf.get("sourcepath"),
                conf.get("suspiciouspath"),
                conf.get("output"),
                conf.get("vocabulary"));
        // configuration example (used as default):
        // job.setSimilarityFunction(CosineSimilarity.class);
        
        job.waitForCompletion(true);
    }
}
