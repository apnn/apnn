package RobustEval;

import SimilarityFile.SimilarityWritable;
import io.github.htools.lib.Log;
import io.github.htools.hadoop.Conf;
import io.github.htools.hadoop.Job;
import io.github.htools.hadoop.io.DelayedWritable;
import io.github.htools.hadoop.io.StringInputFormat;
import io.github.htools.io.HDFSPath;
import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

/**
 * Extract the document frequency of terms from a Wikipedia XML source file
 *
 * @author jeroen
 */
public class RebuildFDMScores {

    private static final Log log = new Log(RebuildFDMScores.class);

    public static void main(String[] args) throws Exception {

        Conf conf = new Conf(args, "-i input -o output");
        conf.setMapMemoryMB(4096);
        conf.setTaskTimeout(1800000);
        conf.setReduceSpeculativeExecution(false);

        HDFSPath input = conf.getHDFSPath("input");
        Path out = new Path(conf.get("output"));

        Job job = new Job(conf, input, out);
        job.setMaxMapAttempts(2);
        job.setInputFormatClass(ResultInputFormat.class);
        ResultInputFormat.addInputPath(job, input);

        job.setNumReduceTasks(100);
        job.setMapperClass(RebuildFDMScoresMap.class);
        job.setReducerClass(RebuildFDMScoresReduce.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(SimilarityWritable.class);
        job.setPartitionerClass(HashPartitioner.class);
        job.setOutputFormatClass(NullOutputFormat.class);

        job.waitForCompletion(true);
    }
}
