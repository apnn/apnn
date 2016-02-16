package RepackTFIDF2;

import io.github.htools.hadoop.Conf;
import io.github.htools.hadoop.Job;
import io.github.htools.hadoop.io.StringInputFormat;
import io.github.htools.io.HDFSPath;
import io.github.htools.lib.Log;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

import java.io.IOException;
import java.util.ArrayList;

/**
 * Extract the document frequency of terms from a Wikipedia XML source file
 * @author jeroen
 */

public class RepackJob {

    private static final Log log = new Log(RepackJob.class);

    public static void main(String[] args) throws Exception {

        Conf conf = new Conf(args, "-i input -o output -v vocabulary");
        conf.setMapMemoryMB(8192);
        conf.setTaskTimeout(1800000);
        conf.setMapSpeculativeExecution(false);

        String input = conf.get("input");
        Path out = new Path(conf.get("output"));

        Job job = new Job(conf, input, out);
        job.setMaxMapAttempts(4);
        setupInput(job, input);

        job.setNumReduceTasks(0);
        job.setMapperClass(RepackMap.class);
        job.setOutputFormatClass(NullOutputFormat.class);
        
        job.waitForCompletion(true);
    }
    
    static void setupInput(Job job, String input) throws IOException {
        job.setInputFormatClass(StringInputFormat.class);
        ArrayList<String> sourceFiles = new ArrayList();

        // get lists of files under the paths of sources and suspicious on HDFS
        for (String inputPath : input.split(",")) {
           HDFSPath sourcepath = new HDFSPath(job.getConfiguration(), inputPath);
           sourceFiles.addAll(sourcepath.getFilepathnames());
        }
        
        // add all possible combinations of a sourceFile with a SuspiciousFile
        // to the input that is mapped.
        for (String sourceFile : sourceFiles) {
                StringInputFormat.add(job, sourceFile);
        }
    }
    
}
