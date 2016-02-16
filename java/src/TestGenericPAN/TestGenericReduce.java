package TestGenericPAN;

import TestGenericMR.*;
import SimilarityFile.IndexSimilarity;
import SimilarityFile.MeasureSimilarity;
import SimilarityFile.SimilarityFile;
import SimilarityFile.SimilarityWritable;
import TestGeneric.Candidate;
import io.github.htools.collection.TopK;
import io.github.htools.collection.TopKMap;
import io.github.htools.hadoop.Conf;
import io.github.htools.hadoop.ContextTools;
import io.github.htools.io.Datafile;
import io.github.htools.io.HDFSPath;
import io.github.htools.lib.DoubleTools;
import io.github.htools.lib.Log;
import static io.github.htools.lib.PrintTools.sprintf;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * reduces all scored similarities between suspicious documents (=key) and all
 * source documents, keeping only the k-most similar source documents per
 * suspicious document.
 *
 * @author jeroen
 */
public class TestGenericReduce extends TestGenericMR.TestGenericReduce {

    public static final Log log = new Log(TestGenericReduce.class);
    HDFSPath outPath;
    HDFSPath gtPath;

    @Override
    public void setupOutput(Context context) {
        outPath = conf.getHDFSPath("output");
        gtPath = conf.getHDFSPath("gt");
    }

    public void writeSimilarity(Candidate candidate) throws IOException {
        String filename = sprintf("suspicious-document%05d.xml", Integer.parseInt(candidate.query));
        if (gtPath.getSubdir(filename).existsFile()) {
            HDFSPath.copy(outPath.getFileSystem(), gtPath.getSubdir(filename), outPath.getSubdir(filename));
        }
    }

    public void closeOutput() {
    }
}
