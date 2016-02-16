package Eval;

import Eval.EvalMap.Result;
import io.github.htools.lib.Log;
import io.github.htools.hadoop.Conf;
import io.github.htools.hadoop.InputFormat;
import io.github.htools.hadoop.Job;
import io.github.htools.hadoop.io.StringInputFormat;
import io.github.htools.io.Datafile;
import io.github.htools.io.HDFSPath;
import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.io.DoubleWritable;
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
public class EvalJob {

    private static final Log log = new Log(EvalJob.class);

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Conf conf = new Conf(args, "path output groundtruth runs metrics ranks");

        Job job = new Job(conf);

        job.setMapperClass(EvalMap.class);
        job.setMapOutputKeyClass(Result.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        job.setNumReduceTasks(1);
        job.setReducerClass(EvalReduce.class);
        job.setOutputFormatClass(NullOutputFormat.class);

        addInputFormat(job, conf.get("path"), conf.getStrings("runs"));
        job.waitForCompletion(true);
    }

    private static void addInputFormat(Job job, String path, String[] runs) throws IOException {
        job.setInputFormatClass(StringInputFormat.class);
        HDFSPath resultspath = new HDFSPath(job.getConfiguration(), path);
        // get lists of files under the paths of sources and suspicious on HDFS
        for (String run : runs) {
            HDFSPath sourcepath = resultspath.getSubdir(run);
            if (!sourcepath.existsDir())
                sourcepath = new HDFSPath(job.getConfiguration(), run);
            ArrayList<Datafile> sourceFiles = sourcepath.getFiles();

        // add all possible combinations of a sourceFile with a SuspiciousFile
            // to the input that is mapped.
            if (InputFormat.getSplitSize(job) >= Long.MAX_VALUE) {
                for (Datafile sourceFile : sourceFiles) {
                    StringInputFormat.add(job, sourceFile.getCanonicalPath());
                }
            }
        }
    }
}
