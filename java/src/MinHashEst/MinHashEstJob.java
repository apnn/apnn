package MinHashEst;

import MinHash.MinHashJob;
import SimilarityFunction.CosineSimilarityTFIDF;
import TestGenericMR.TestGenericMapIE;
import TestGenericMR.TestGenericReduceIE;
import io.github.htools.lib.Log;
import io.github.htools.hadoop.Conf;

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
 * -h: (optional) number of hash functions to use (default=240)
 * -b: (optional) number of hash functions to be combined in one band (default=5)
 * @author Jeroen
 */
public class MinHashEstJob {

    private static final Log log = new Log(MinHashEstJob.class);
    
    public static void main(String[] args) throws Exception {

        Conf conf = new Conf(args, "sourcepath suspiciouspath output");
        
        MinHashJob job = new MinHashJob(conf,
                conf.get("sourcepath"),
                conf.get("suspiciouspath"),
                conf.get("output")
        );
                
        job.setAnnIndex(AnnMinHashEst.class);
        job.setMapperClass(TestGenericMapIE.class);
        job.setReducerClass(TestGenericReduceIE.class);
        
        // configuration example (used as default):
        // job.setTopK(100);
        job.setSimilarityFunction(CosineSimilarityTFIDF.class);
        
        job.waitForCompletion(true);
    }
}