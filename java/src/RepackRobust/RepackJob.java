package RepackRobust;

import io.github.htools.hadoop.Conf;
import io.github.htools.hadoop.Job;
import io.github.htools.hadoop.io.DatafileInputFormat;
import io.github.htools.io.HDFSPath;
import io.github.htools.lib.Log;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

import java.io.IOException;
import java.util.ArrayList;

/**
 * Repacks the Robust 2004 documents by removing non-text content
 * and repacking the files in tar.lz4 files. The resulting files will contain the
 * actual text, including stopwords, no reading marks.
 *
 * parameters:
 * input: folder containing the original collection in tar.lz4 format
 * output: folder that will contain the repacked files.
 * @author jeroen
 */
public class RepackJob {

    private static final Log log = new Log(RepackJob.class);

    public static void main(String[] args) throws Exception {

        Conf conf = new Conf(args, "-i input -o output");
        conf.setMapMemoryMB(4096);
        conf.setTaskTimeout(180000);
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

    /**
     * add all files in the given paths as input.
     */
    static void setupInput(Job job, String input) throws IOException {
        job.setInputFormatClass(DatafileInputFormat.class);
        ArrayList<String> sourceFiles = new ArrayList();

        for (String inputPath : input.split(",")) {
           HDFSPath sourcepath = new HDFSPath(job.getConfiguration(), inputPath);
           sourceFiles.addAll(sourcepath.getFilepathnames());
        }

        for (String sourceFile : sourceFiles) {
                DatafileInputFormat.addDirs(job, sourceFile);
        }
    }
    
}
