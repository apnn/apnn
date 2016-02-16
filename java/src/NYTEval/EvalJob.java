package NYTEval;

import io.github.htools.hadoop.Conf;
import io.github.htools.hadoop.Job;
import io.github.htools.io.HDFSPath;
import io.github.htools.lib.Log;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

import static NYTEval.EvalReport.report;

/**
 * Extract the document frequency of terms from a Wikipedia XML source file
 * @author jeroen
 */

public class EvalJob {

    private static final Log log = new Log(EvalJob.class);

    public static void main(String[] args) throws Exception {

        Conf conf = new Conf(args, "-i input -o output");
        conf.setTaskTimeout(1800000);
        conf.setMapSpeculativeExecution(false);

        HDFSPath in = conf.getHDFSPath("input");
        HDFSPath out = conf.getHDFSPath("output");
        out.trash();

        Job job = new Job(conf, in, out);
        job.setMaxMapAttempts(2);

        job.setNumReduceTasks(100);
        job.setMapperClass(EvalMap.class);
        job.setReducerClass(EvalReduce.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(ResultWritable.class);
        job.setOutputFormatClass(NullOutputFormat.class);
        job.setInputFormatClass(ResultInputFormat.class);
        job.setGroupingComparatorClass(Text.Comparator.class);
        ResultInputFormat.addInputPath(job, in);
        job.waitForCompletion(true);
        report(out);
    }
}
