package TestGenericRobust;

import TestGenericMR.*;
import BruteForce.AnnBruteForce;
import SimilarityFunction.CosineSimilarityTFIDF;
import TestGeneric.Candidate;
import static TestGenericMR.TestGenericJob.getScanTopK;
import static TestGenericMR.TestGenericJob.getTopK;
import io.github.htools.lib.Log;
import io.github.htools.hadoop.Conf;
import io.github.htools.io.Datafile;
import io.github.htools.io.HDFSPath;
import static io.github.htools.lib.PrintTools.sprintf;
import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

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
public class TestGenericFDMJob extends TestGenericJob {

    private static final Log log = new Log(TestGenericFDMJob.class);

    public TestGenericFDMJob(Conf conf, String source, String queries, String output, String vocabulary, String fdmscores) throws IOException {
        super(conf, source, queries, output, vocabulary);
        getConfiguration().set("fdmscores", fdmscores);
        getConfiguration().set("source", source);
        getConfiguration().set("output", output);
        getConfiguration().set("vocabulary", vocabulary);
        getConfiguration().set("queries", queries);
        setMapperClass(TestGenericFDMMap.class);
        setMapOutputKeyClass(Text.class);
        setMapOutputValueClass(Candidate.class);
        setNumReduceTasks(250);
        setReducerClass(TestGenericFDMReduce.class);
        setOutputFormatClass(NullOutputFormat.class);

        if (!conf.containsKey(ANNINDEXCLASS)) {
            setAnnIndex(getConfiguration(), AnnBruteForce.class);
        }
    }

    @Override
    protected void setupInputFormat(String sources, String queries) throws IOException {
        BfFDMJob.setupInputFormat(this, sources);
    }
    
    public void mergeResults() throws IOException {
        HDFSPath out = new HDFSPath(conf, conf.get("output"));
        Datafile f = new Datafile(conf, conf.get("output") + ".file");
        HDFSPath.mergeFiles(f, out.getFiles());
        out.trash();
        f.move(new Datafile(conf, conf.get("output")));
    }
    
    @Override
    protected void addParameters(Configuration conf, ArrayList<String> parameters) {
    }
}
