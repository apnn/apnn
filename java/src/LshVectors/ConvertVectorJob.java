package LshVectors;

import io.github.htools.hadoop.Conf;
import io.github.htools.hadoop.Job;
import io.github.htools.lib.Log;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

/**
 * Counts the document frequency of terms in  Wikipedia corpus
 * 
 * @author Jeroen
 */
public class ConvertVectorJob {

    private static final Log log = new Log(LshVectors.WpVectorJob.class);

    public static void main(String[] args) throws Exception {

        Conf conf = new Conf(args, "-i input -o output -v vocabulary");
        conf.setMapMemoryMB(8196);
        conf.setTaskTimeout(60000000);
         conf.getHDFSPath("output").trash();

        Job job = new Job(conf, conf.get("input"), conf.get("output"));
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job, conf.getHDFSPath("input"));

        job.setNumReduceTasks(0);
        job.setMapperClass(ConvertVectorMap.class);
        job.setOutputFormatClass(NullOutputFormat.class);
        job.waitForCompletion(true);
    }
}
