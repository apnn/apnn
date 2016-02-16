package BruteForceNYT;

import BruteForce.AnnBruteForceII;
import TestGenericMR.TestGenericJob;
import TestIG.TestIGJob;
import io.github.htools.hadoop.Conf;
import io.github.htools.lib.Log;

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
public class BruteForceNYTIGJob {

    private static final Log log = new Log(BruteForceNYTIGJob.class);

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Conf conf = new Conf(args, "sourcepath output vocabulary -k [topk]");
        conf.setTaskTimeout(180000000);
        TestGenericJob.setAnnIndex(conf, AnnBruteForceII.class);
        conf.getHDFSPath("output").trash();

        TestIGJob job;
        job = new TestIGJob(conf,
                conf.get("sourcepath"),
                conf.get("output"),
                conf.get("vocabulary"));
        // configuration example (used as default):
        // job.setSimilarityFunction(CosineSimilarity.class);
        
        job.waitForCompletion(true);
        job.mergeResults();
    }
}
