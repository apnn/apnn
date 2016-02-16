package RobustQlFdm;

import io.github.htools.lib.Log;
import io.github.htools.hadoop.Conf;
import io.github.htools.hadoop.Job;
import io.github.htools.hadoop.io.DatafileInputFormat;
import io.github.htools.hadoop.io.IntDoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

/**
 * Extract the document frequency of terms from a Wikipedia XML source file
 * @author jeroen
 */

public class QlFdmJob {

    private static final Log log = new Log(QlFdmJob.class);

    public static void main(String[] args) throws Exception {

        Conf conf = new Conf(args, "-q ql -f fdm -k k -o output");
        conf.setMapMemoryMB(4096);
        conf.setTaskTimeout(18000000);
        conf.setMapSpeculativeExecution(false);

        Job job = new Job(conf);
        job.setMaxMapAttempts(2);
        job.setInputFormatClass(DatafileInputFormat.class);
        DatafileInputFormat.addInputPath(job, conf.getHDFSPath("fdm"));
        
        job.setNumReduceTasks(1);
        job.setMapperClass(QlFdmMap.class);
        job.setReducerClass(QlFdmReducer.class);
        job.setMapOutputKeyClass(IntDoubleWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputFormatClass(NullOutputFormat.class);
        job.setJobName(conf.get("ql"), conf.get("fdm"), conf.get("k"), conf.get("output"));
        job.waitForCompletion(true);
    }
}
