package TestGenericPAN;

import io.github.htools.lib.Log;
import io.github.htools.hadoop.Conf;
import java.io.IOException;

/**
 * Computes the cosine similarity between all suspicious and source documents of
 * the PAN11 collection. This class retrieves the k-most similar source
 * documents given a suspicious document, optionally set k (default=100) as the
 * maximum number of most similar source documents to retrieve and set the
 * similarity function (default=CosineSimilarity) that is used to score the
 * similarity between two documents.
 *
 * The default mapper does a brute force comparison suspicious document with all
 * source documents.
 *
 * The default reducer keeps only the k-most similar source document per
 * suspicious document and stores the result in a SimilarityFile. Override the
 * configured Mapper and Reducer to change the default operation.
 *
 * parameters:
 *
 * sources: HDFS path containing the PAN11 source documents wrapped in
 * ArchiveFiles (e.g. .tar.lz4) suspicious: HDFS path containing the PAN11
 * suspicious documents wrapped in ArchiveFiles (e.g. .tar.lz4) output: the
 * resulting k-most similar source documents per suspicious document are written
 * to a file with this name in SimilarityFile format
 *
 * @author Jeroen
 */
public class TestGenericJob extends TestGenericMR.TestGenericJob {

    private static final Log log = new Log(TestGenericJob.class);

    public TestGenericJob(Conf conf, String source, String query, String output, String vocabulary, String gt) throws IOException {
        super(conf, source, query, output, vocabulary);
        this.getConfiguration().set("gt", gt);
        this.setReducerClass(TestGenericPAN.TestGenericReduce.class);
    }
}
