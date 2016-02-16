package VocabularyPAN;

import io.github.htools.hadoop.Conf;
import io.github.htools.hadoop.InputFormat;
import io.github.htools.hadoop.Job;
import io.github.htools.hadoop.io.DatafileInputFormat;
import io.github.htools.io.HDFSPath;
import io.github.htools.lib.Log;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

import java.io.IOException;
import java.util.ArrayList;

/**
 * Construct a vocabulary for the PAN11 collection, using the extracted text
 * by RepackPAN11. Per term, the document frequency and collection frequency
 * are stored.
 *
 * parameters:
 * input: folder that contains the repacked collection files
 * output: filename for the vocabulary file
 */
public class VocabularyJob {

    private static final Log log = new Log(VocabularyJob.class);

    public static void main(String[] args) throws Exception {

        Conf conf = new Conf(args, "-i input -o output");
        conf.setMapMemoryMB(4096);
        conf.setTaskTimeout(180000);
        conf.setMapSpeculativeExecution(false);
        conf.setMaxMapAttempts(1);

        String input = conf.get("input");
        Path out = new Path(conf.get("output"));

        Job job = new Job(conf, input, out);
        InputFormat.setNonSplitable(job);
        setupInput(job, input);

        job.setNumReduceTasks(1);
        job.setMapperClass(VocabularyMap.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setReducerClass(VocabularyReduce.class);
        job.setGroupingComparatorClass(Text.Comparator.class);
        job.setOutputFormatClass(NullOutputFormat.class);
        
        job.waitForCompletion(true);
    }

    /**
     * add all files in the given paths as input.
     */
    public static void setupInput(Job job, String input) throws IOException {
        job.setInputFormatClass(DatafileInputFormat.class);
        ArrayList<String> sourceFiles = new ArrayList();

        // get lists of files under the paths of sources and suspicious on HDFS
        for (String inputPath : input.split(",")) {
           HDFSPath sourcepath = new HDFSPath(job.getConfiguration(), inputPath);
           sourceFiles.addAll(sourcepath.getFilepathnames());
        }
        
        // add all possible combinations of a sourceFile with a SuspiciousFile
        // to the input that is mapped.
        for (String sourceFile : sourceFiles) {
            DatafileInputFormat.addDirs(job, sourceFile);
        }
    }
    
}
