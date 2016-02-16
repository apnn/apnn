package LSHCosOld;

import TestGenericNYT.TestGenericNYTJob;
import io.github.htools.hadoop.Conf;
import io.github.htools.lib.Log;
import org.apache.hadoop.conf.Configuration;

/**
 * Basis for this method is to create 4-byte hashCodes of all s-character
 * Shingles (consecutive substrings) not starting with a space. Stop words are
 * included. If a suspicious document and a source document have a (hashCode of
 * a) shingle in common, they are compared.
 *
 * parameters:
 *
 * sourcepath: HDFS path containing the PAN11 source documents wrapped in
 * ArchiveFiles (e.g. .tar.lz4) suspiciouspath: HDFS path containing the PAN11
 * suspicious documents wrapped in ArchiveFiles (e.g. .tar.lz4) output: the
 * resulting k-most similar source documents per suspicious document are written
 * to a file with this name in SimilarityFile format -s: shingleSize (default=9)
 * number of characters to construct shingles
 *
 * @author Jeroen
 */
public class LSHCosNYTJob {

    private static final Log log = new Log(LSHCos.LSHCosNYTJob.class);
    public static final String NUMHYPERPLANES = "numhyperplanes";

    public static void main(String[] args) throws Exception {

        Conf conf = new Conf(args, "sourcepath output -v vocabulary -s vocsize --noninteractive");
        conf.setTaskTimeout(30000000);
        conf.getHDFSPath("output").trash();

        TestGenericNYTJob job = new TestGenericNYTJob(conf,
                conf.get("sourcepath"),
                conf.get("output"),
                conf.get("vocabulary")
        );
        job.setMapperClass(LSHCosNYTMap.class);

        // don't remove stopwords when creating shingles
        //job.setTokenizer(Tokenizer.class);

        job.submitJob();
    }

    /**
     * @param conf
     * @return the consecutive number of characters to use as a shingle
     * (default=9)
     * @throws ClassNotFoundException
     */
    public static int getNumHyperplanes(Configuration conf) {
        return conf.getInt(NUMHYPERPLANES, 100);
    }
}
