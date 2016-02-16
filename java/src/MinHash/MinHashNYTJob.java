package MinHash;

import SimilarityFile.IndexSimilarity;
import TestGeneric.AnnIndex;
import TestGenericMR.TestGenericJob;
import TestGenericNYT.TestGenericNYTJob;
import io.github.htools.hadoop.Conf;
import io.github.htools.lib.Log;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
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
 * ArchiveFiles (e.g. .tar.lz4) 
 * suspiciouspath: HDFS path containing the PAN11
 * suspicious documents wrapped in ArchiveFiles (e.g. .tar.lz4)
 * output: the resulting k-most similar source documents per suspicious document
 * are written to a file with this name in SimilarityFile format
 * hashfunctions=#: (optional) number of hash functions to use (default=240)
 * bandwidth=#: (optional) number of hash functions to be combined in one band (default=1)
 * @author Jeroen
 */
public class MinHashNYTJob extends TestGenericNYTJob {

    private static final Log log = new Log(MinHashNYTJob.class);
    public static final String MINHASHFUNCTIONS = "hashfunctions";
    public static final String MINHASHBANDWDITH = "bandwidth";

    public MinHashNYTJob(Conf conf, String sources, String output, String vocabulary) throws IOException, NoSuchMethodException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
        super(conf, sources, output, vocabulary);
    }
    
    @Override
    protected void addParameters(Configuration conf, ArrayList<String> parameters) {
        parameters.add(sprintf("hashfunctions=%s", conf.get("hashfunctions")));
        parameters.add(sprintf("bandwidth=%s", conf.get("bandwidth")));
    }
    
    public static void main(String[] args) throws Exception {

        Conf conf = new Conf(args, "sourcepath output -v vocabulary -h hashfunctions -b bandwidth --noninteractive");
        TestGenericNYTJob.setAnnIndex(conf, AnnMinHash.class);
        
        AnnMinHash m = new AnnMinHash(IndexSimilarity.singleton, conf);
        AnnIndex n = TestGenericJob.getAnnIndex(IndexSimilarity.singleton, conf);
        MinHashNYTJob job = new MinHashNYTJob(conf,
                conf.get("sourcepath"),
                conf.get("output"),
                conf.get("vocabulary")
        );
        
        job.useDocumentNYTTFIDF2();
                
        job.submitJob();
    }
    
    /**
     * @param conf
     * @return the number of hash functions to use (default=200)
     * @throws ClassNotFoundException
     */
    public static int getNumHashFunctions(Configuration conf) {
        int minhashfunctions = conf.getInt(MINHASHFUNCTIONS, 240);
        //log.info("getNumHashFuctions %d", minhashfunctions);
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
