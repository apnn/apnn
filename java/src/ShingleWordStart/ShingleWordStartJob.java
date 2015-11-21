package ShingleWordStart;

import TestGeneric.Tokenizer;
import TestGenericMR.TestGenericJob;
import io.github.htools.lib.Log;
import io.github.htools.hadoop.Conf;

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
public class ShingleWordStartJob {

    private static final Log log = new Log(ShingleWordStartJob.class);

    public static void main(String[] args) throws Exception {

        Conf conf = new Conf(args, "sourcepath suspiciouspath output -v vocabulary -s shinglesize");

        TestGenericJob.setAnnIndex(conf, AnnShingleWordStart.class);

        TestGenericJob job = new TestGenericJob(conf,
                conf.get("sourcepath"),
                conf.get("suspiciouspath"),
                conf.get("output"),
                conf.get("vocabulary")
        );

        job.useDocumentContent();
        job.waitForCompletion(true);
    }
}
