package MinHash;

import SimilarityFile.IndexSimilarity;
import SimilarityFile.SimilarityFile;
import SimilarityFunction.CosineSimilarityTFIDF;
import TestGeneric.AnnIndex;
import TestGenericMR.TestGenericJob;
import io.github.htools.lib.Log;
import io.github.htools.hadoop.Conf;
import static io.github.htools.lib.PrintTools.sprintf;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
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
 * ArchiveFiles (e.g. .tar.lz4) 
 * suspiciouspath: HDFS path containing the PAN11
 * suspicious documents wrapped in ArchiveFiles (e.g. .tar.lz4)
 * output: the resulting k-most similar source documents per suspicious document
 * are written to a file with this name in SimilarityFile format
 * hashfunctions=#: (optional) number of hash functions to use (default=240)
 * bandwidth=#: (optional) number of hash functions to be combined in one band (default=1)
 * @author Jeroen
 */
public class MinHashJob extends TestGenericJob {

    private static final Log log = new Log(MinHashJob.class);
    public static final String MINHASHFUNCTIONS = "hashfunctions";
    public static final String MINHASHBANDWDITH = "bandwidth";

    public MinHashJob(Conf conf, String sources, String suspicious, String output, String vocabulary) throws IOException, NoSuchMethodException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
        super(conf, sources, suspicious, output, vocabulary);
    }
    
    @Override
    protected void addParameters(Configuration conf, ArrayList<String> parameters) {
        parameters.add(sprintf("hashfunctions=%s", conf.get("hashfunctions")));
        parameters.add(sprintf("bandwidth=%s", conf.get("bandwidth")));
    }
    
    public static void main(String[] args) throws Exception {

        Conf conf = new Conf(args, "sourcepath suspiciouspath output -v vocabulary -h hashfunctions -b bandwidth");
        MinHashJob.setAnnIndex(conf, AnnMinHash.class);
        
        AnnMinHash m = new AnnMinHash(IndexSimilarity.singleton, conf);
        AnnIndex n = TestGenericJob.getAnnIndex(IndexSimilarity.singleton, conf);
        MinHashJob job = new MinHashJob(conf,
                conf.get("sourcepath"),
                conf.get("suspiciouspath"),
                conf.get("output"),
                conf.get("vocabulary")
        );
                
        job.waitForCompletion(true);
    }
    
    /**
     * @param conf
     * @return the number of hash functions to use (default=200)
     * @throws ClassNotFoundException
     */
    public static int getNumHashFunctions(Configuration conf) {
        int minhashfunctions = conf.getInt(MINHASHFUNCTIONS, 240);
        log.info("getNumHashFuctions %d", minhashfunctions);
        return minhashfunctions;
    }    

    /**
     * @param conf
     * @return the number of hash functions to use (default=200)
     * @throws ClassNotFoundException
     */
    public static int getBandwidth(Configuration conf) {
        return conf.getInt(MINHASHBANDWDITH, 1);
    }    
}
