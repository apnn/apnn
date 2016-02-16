package TestGenericNYT;

import BruteForce.AnnBruteForce;
import SimilarityFile.SimilarityOutputFormat;
import SimilarityFile.SimilarityWritable;
import TestGeneric.Candidate;
import TestGenericMR.TestGenericJob;
import io.github.htools.hadoop.Conf;
import io.github.htools.hadoop.io.FilePairInputFormat;
import io.github.htools.io.Datafile;
import io.github.htools.io.HDFSPath;
import io.github.htools.lib.Log;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

import java.io.IOException;
import java.util.ArrayList;

/**
 * Computes the cosine similarity between all documents of
 * the NYT collection. This class retrieves the k-most similar source
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
 * sources: HDFS path containing the NYT source documents wrapped in
 * ArchiveFiles (e.g. .tar.lz4) 
 * output: the resulting k-most similar source documents per suspicious document are written
 * to a file with this name in SimilarityFile format
 *
 * @author Jeroen
 */
public class TestGenericNYTJob extends TestGenericJob {

    private static final Log log = new Log(TestGenericNYTJob.class);

    public TestGenericNYTJob(Conf conf, String source, String output, String vocabulary) throws IOException {
        super(conf, source, "", output, vocabulary);
        getConfiguration().set("source", source);
        getConfiguration().set("output", output);
        getConfiguration().set("vocabulary", vocabulary);
        setMapperClass(TestGenericNYTMap.class);
        setMapOutputKeyClass(Text.class);
        setMapOutputValueClass(Candidate.class);
        setNumReduceTasks(500);
        setReducerClass(TestGenericNYTReduce.class);
        setOutputFormatClass(SimilarityOutputFormat.class);
        setOutputValueClass(SimilarityWritable.class);
        setOutputKeyClass(NullWritable.class);
        SimilarityOutputFormat.setOutputPath(this, conf.getHDFSPath("output"));
        setPartitionerClass(HashPartitioner.class);
        
        if (!conf.containsKey(ANNINDEXCLASS)) {
            setAnnIndex(getConfiguration(), AnnBruteForce.class);
        }
    }

    @Override
    protected void setupInputFormat(String sources, String query) throws IOException {
        log.info("setupInputFormat");
       setInputFormatClass(FilePairInputFormat.class);
        FilePairInputFormat.setSplitable(false);

        // get lists of files under the paths of sources and suspicious on HDFS
        HDFSPath sourcepath = new HDFSPath(this.getConfiguration(), sources);
        ArrayList<Datafile> sourceFiles = sourcepath.getFiles();

        for (int i = 0; i < sourceFiles.size(); i++) {
            for (int j = 0; j < sourceFiles.size(); j++) {
               int id = i * 2 + (j % 2);
               FilePairInputFormat.add(this, id + "", sourceFiles.get(i).getCanonicalPath(),
                       sourceFiles.get(j).getCanonicalPath());
            }
        }
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
