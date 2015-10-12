package MinHash;

import TestAnnMR.TestAnnJob;
import io.github.htools.lib.Log;
import io.github.htools.hadoop.Conf;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;

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
public class MinHashJob {

    private static final Log log = new Log(MinHashJob.class);
    public static final String MINHASHFUNCTIONS = TestAnnJob.class.getCanonicalName() + ".HashFunctions";
    public static final String MINHASHBANDWDITH = TestAnnJob.class.getCanonicalName() + ".Bandwidth";

    public static void main(String[] args) throws Exception {

        Conf conf = new Conf(args, "sourcepath suspiciouspath output -h [numhashfunctions] -b [bandwidth]");

        TestAnnJob job = new TestAnnJob(conf,
                conf.get("sourcepath"),
                conf.get("suspiciouspath"),
                conf.get("output")
        );
        
        job.setAnnIndex(AnnMinHash.class);
        
        // configuration example (used as default):
        // job.setTopK(100);
        // job.setSimilarityFunction(CosineSimilarity.class);
        setNumHashFunctions(job, conf.getInt("numhashfunctions", 240));
        setBandwidth(job, conf.getInt("bandwidth", 5));
        
        job.waitForCompletion(true);
    }
    
    /**
     * Configure the number of hash functions to use
     * @param job
     * @param numHashFunctions
     */
    public static void setNumHashFunctions(Job job, int numHashFunctions) {
        job.getConfiguration().setInt(MINHASHFUNCTIONS, numHashFunctions);
    }

    /**
     * @param conf
     * @return the number of hash functions to use (default=200)
     * @throws ClassNotFoundException
     */
    public static int getNumHashFunctions(Configuration conf) {
        return conf.getInt(MINHASHFUNCTIONS, 240);
    }    
    
    /**
     * Configure the number of hash functions to use
     * @param job
     * @param bandwidth
     */
    public static void setBandwidth(Job job, int bandwidth) {
        job.getConfiguration().setInt(MINHASHBANDWDITH, bandwidth);
    }

    /**
     * @param conf
     * @return the number of hash functions to use (default=200)
     * @throws ClassNotFoundException
     */
    public static int getBandwidth(Configuration conf) {
        return conf.getInt(MINHASHBANDWDITH, 5);
    }    
}
