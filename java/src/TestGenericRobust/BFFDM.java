package TestGenericRobust;

import io.github.htools.lib.Log;
import io.github.htools.hadoop.Conf;

/**
 * Computes the normalized information gain between all suspicious and source
 * documents of the PAN11 collection. The k (default=100) highest nIG per
 * suspicious document are stored.
 *
 * parameters:
 *
 * sourcepath: HDFS path containing the PAN11 source documents wrapped in
 * ArchiveFiles (e.g. .tar.lz4)
 *
 * suspiciouspath: HDFS path containing the PAN11 suspicious documents wrapped
 * in ArchiveFiles (e.g. .tar.lz4)
 *
 * output: the resulting k-most similar source documents per suspicious document
 * are written to a file with this name in SimilarityFile format
 *
 * @author Jeroen
 */
public class BFFDM {

    private static final Log log = new Log(BFFDM.class);

    public static void main(String[] args) throws Exception {

        Conf conf = new Conf(args, "source queries output fdmvoc");
        conf.setTaskTimeout(1800000);
        
        BfFDMJob job = new BfFDMJob(conf,
                conf.get("source"),
                conf.get("queries"),
                conf.get("output"),
                conf.get("fdmvoc"));


        job.waitForCompletion(true);
    }
}
