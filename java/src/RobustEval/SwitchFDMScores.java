package RobustEval;

import io.github.htools.hadoop.Conf;
import io.github.htools.hadoop.Job;
import io.github.htools.hadoop.io.StringInputFormat;
import io.github.htools.io.Datafile;
import io.github.htools.io.HDFSPath;
import io.github.htools.lib.Log;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;

/**
 * Extract the document frequency of terms from a Wikipedia XML source file
 *
 * @author jeroen
 */
public class SwitchFDMScores {

    private static final Log log = new Log(SwitchFDMScores.class);

    public static void main(String[] args) throws Exception {

        Conf conf = new Conf(args, "-i {results} -f fdmscores -o output -e endresult");
        conf.setMapMemoryMB(4096);
        conf.setTaskTimeout(1800000);
        conf.setMapSpeculativeExecution(false);

        String input = conf.get("input");
        HDFSPath out = conf.getHDFSPath("output");
        HashSet<String> existingFiles = new HashSet(conf.getHDFSPath("endresult").getFilenames());
        out.trash();

        Job job = new Job(conf, input, out);
        job.setMaxMapAttempts(1);
        for (String path : conf.getStrings("results")) {
            setupInput(job, "result", path, existingFiles);
        }
        setupInput(job, "fdm", conf.get("fdmscores"), new HashSet<String>());

        job.setNumReduceTasks(500);
        job.setMapperClass(SwitchFDMScoresMap.class);
        job.setReducerClass(SwitchFDMScoresReduce.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(ResultWritable.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(ResultWritable.class);
        ResultOutputFormat.setOutputPath(job, out);
        job.setOutputFormatClass(ResultOutputFormat.class);
        job.setPartitionerClass(HashPartitioner.class);

        job.waitForCompletion(true);
    }

    static void setupInput(Job job, String prefix, String inputPath, HashSet<String> existingFiles) throws IOException {
        job.setInputFormatClass(StringInputFormat.class);
        StringInputFormat.setSplitable(true);
        ArrayList<String> sourceFiles = new ArrayList();
        HDFSPath sourcepath = new HDFSPath(job.getConfiguration(), inputPath);
        for (Datafile df : sourcepath.getFiles()) {
            if (!existingFiles.contains(df.getName())) {
                sourceFiles.add(df.getCanonicalPath());
                log.info("%s", df.getCanonicalPath());
            }
        }
        for (String sourceFile : sourceFiles) {
            StringInputFormat.add(job, prefix, sourceFile);
        }
    }
}
