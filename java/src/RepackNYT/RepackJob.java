package RepackNYT;

import io.github.htools.hadoop.Conf;
import io.github.htools.hadoop.Job;
import io.github.htools.hadoop.io.DatafileInputFormat;
import io.github.htools.io.HDFSPath;
import io.github.htools.lib.Log;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

import java.io.IOException;
import java.util.ArrayList;

/**
 * Repacks the NYT documents by removing non-text content (e.g. boilerplate, urls)
 * and repacking the files in tar.lz4 files. The resulting files will contain the
 * actual text, including stopwords and reading marks.
 *
 * parameters:
 * input: folder containing the original collection in tar.lz4 format
 * ids: list of NYT ids of documents that have been annotated (rest is ignored)
 * modified this after an ids file was obtained, for a first run this should be
 * modified to output only files that have been annotated.
 * gt: file that will contain the annotated labels for NYT
 * output: folder that will contain the repacked files.
 * @author jeroen
 */

public class RepackJob {

    private static final Log log = new Log(RepackJob.class);

    public static void main(String[] args) throws Exception {

        Conf conf = new Conf(args, "-i input -d ids -o output -g gt");
        conf.setMapMemoryMB(4096);
        conf.setTaskTimeout(1800000);
        conf.setMapSpeculativeExecution(false);

        String input = conf.get("input");
        Path out = new Path(conf.get("output"));

        Job job = new Job(conf, input, out);
        job.setMaxMapAttempts(1);
        setupInput(job, input);

        job.setNumReduceTasks(1);
        job.setMapperClass(RepackMap.class);
        job.setReducerClass(RepackReduce.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
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
