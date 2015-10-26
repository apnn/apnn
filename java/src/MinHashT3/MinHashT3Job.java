package MinHashT3;

import MinHash.*;
import TestAnnMR.TestAnnJob;
import TestGeneric.Tokenizer;
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
public class MinHashT3Job {

    private static final Log log = new Log(MinHashT3Job.class);
    public static final String MINHASHFUNCTIONS = TestAnnJob.class.getCanonicalName() + ".hashfunctions";
    public static final String SINGLESIZE = TestAnnJob.class.getCanonicalName() + ".shinglesize";

    public static void main(String[] args) throws Exception {

        Conf conf = new Conf(args, "sourcepath suspiciouspath output -h [numhashfunctions] -s [shinglesize]");

        TestAnnJob job = new TestAnnJob(conf,
                conf.get("sourcepath"),
                conf.get("suspiciouspath"),
                conf.get("output")
        );
        
        job.setAnnIndex(AnnMinHashT3.class);
        
        // configuration example (used as default):
        // job.setTopK(100);
        // job.setSimilarityFunction(CosineSimilarity.class);
        setNumHashFunctions(job, conf.getInt("numhashfunctions", 240));
        setShingleSize(job, conf.getInt("shinglesize", 3));
        
        // use a tokenizer that does not remove stopwords
        job.setTokenizer(Tokenizer.class);
        
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
    public static void setShingleSize(Job job, int bandwidth) {
        job.getConfiguration().setInt(SINGLESIZE, bandwidth);
    }

    /**
     * @param conf
     * @return the number of hash functions to use (default=200)
     * @throws ClassNotFoundException
     */
    public static int getShingleSize(Configuration conf) {
        return conf.getInt(SINGLESIZE, 3);
    }    
}
