package ShingleT5;

import TestGeneric.ContentExtractorNYT;
import TestGenericMR.TestGenericJob;
import TestGenericNYT.TestGenericNYTJob;
import io.github.htools.hadoop.Conf;
import io.github.htools.lib.Log;

/**
 * Basis for this method is to create 4-byte hashCodes of all s-term Shingles
 * (consecutive words) starting with a stop word (which are included).
 * If a suspicious document and a source document have a (hashCode of a) shingle
 * in common, they are compared.
 *
 * parameters:
 *
 * sourcepath: HDFS path containing the PAN11 source documents wrapped in
 * ArchiveFiles (e.g. .tar.lz4) 
 * suspiciouspath: HDFS path containing the PAN11
 * suspicious documents wrapped in ArchiveFiles (e.g. .tar.lz4)
 * output: the resulting k-most similar source documents per suspicious document
 * are written to a file with this name in SimilarityFile format
 * -s: shingleSize in words (default=3)
 * @author Jeroen
 */
public class ShingleNYTJob {

    private static final Log log = new Log(ShingleNYTJob.class);
    
    public static void main(String[] args) throws Exception {

        Conf conf = new Conf(args, "sourcepath output -v vocabulary --noninteractive");
        TestGenericJob.setAnnIndex(conf, ShingleT5.class);
        TestGenericJob.setContentExtractor(conf, ContentExtractorNYT.class);
        conf.getHDFSPath("output").trash();

        TestGenericNYTJob job = new TestGenericNYTJob(conf,
                conf.get("sourcepath"),
                conf.get("output"),
                conf.get("vocabulary")
        );

        job.submitJob();
    } 
}
