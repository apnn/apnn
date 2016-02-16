package SplitAnnotated;

import io.github.htools.hadoop.Conf;
import io.github.htools.hadoop.Job;
import io.github.htools.hadoop.io.DatafileInputFormat;
import io.github.htools.io.HDFSPath;
import io.github.htools.io.HPath;
import io.github.htools.lib.Log;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

import java.io.IOException;
import java.util.ArrayList;

/**
 * Extract the document frequency of terms from a Wikipedia XML source file
 * @author jeroen
 */

public class SplitJob {

    private static final Log log = new Log(SplitJob.class);

    public static void main(String[] args) throws Exception {

        Conf conf = new Conf(args, "-i input -d ids -o output");
        conf.setMapMemoryMB(4096);
        conf.setTaskTimeout(1800000);
        conf.setMapSpeculativeExecution(false);

        String input = conf.get("input");
        HDFSPath out = conf.getHDFSPath("output");
        out.trash();

        Job job = new Job(conf, input, out);
        job.setMaxMapAttempts(2);
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.setInputPaths(job, input);

        job.setNumReduceTasks(246);
        job.setMapperClass(SplitMap.class);
        job.setReducerClass(SplitReduce.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputFormatClass(NullOutputFormat.class);
        
        job.waitForCompletion(true);
    }
}
