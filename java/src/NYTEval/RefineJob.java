package NYTEval;

import SimilarityFile.SimilarityInputFormat;
import io.github.htools.hadoop.Conf;
import io.github.htools.hadoop.Job;
import io.github.htools.hadoop.hashjoin.HashPartitioner;
import io.github.htools.io.HDFSPath;
import io.github.htools.lib.Log;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;

import java.io.IOException;
import java.util.ArrayList;

/**
 * Extract the document frequency of terms from a Wikipedia XML source file
 * @author jeroen
 */

public class RefineJob {

    private static final Log log = new Log(RefineJob.class);

    public static void main(String[] args) throws Exception {

        Conf conf = new Conf(args, "-i input -d ids -a annotation -o output");
        conf.setMapMemoryMB(4096);
        conf.setTaskTimeout(1800000);
        conf.setMapSpeculativeExecution(false);

        String input = conf.get("input");
        HDFSPath out = conf.getHDFSPath("output");
        out.trash();

        Job job = new Job(conf, input, conf.get("ids"), conf.get("annotation"), out);
        job.setMaxMapAttempts(2);
        setupInput(job, input);

        job.setNumReduceTasks(200);
        job.setMapperClass(RefineMap.class);
        job.setReducerClass(RefineReduce.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(RefineMap.Result.class);
        job.setOutputFormatClass(ResultOutputFormat.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(ResultWritable.class);
        job.setPartitionerClass(HashPartitioner.class);
        job.setGroupingComparatorClass(IntWritable.Comparator.class);
        ResultOutputFormat.setOutputPath(job, out);
        job.waitForCompletion(true);
    }
    
    static void setupInput(Job job, String input) throws IOException {
        job.setInputFormatClass(SimilarityInputFormat.class);
        ArrayList<String> sourceFiles = new ArrayList();

        // get lists of files under the paths of sources and suspicious on HDFS
        for (String inputPath : input.split(",")) {
           HDFSPath sourcepath = new HDFSPath(job.getConfiguration(), inputPath);
           sourceFiles.addAll(sourcepath.getFilepathnames());
        }
        
        // add all possible combinations of a sourceFile with a SuspiciousFile
        // to the input that is mapped.
        for (String sourceFile : sourceFiles) {
                SimilarityInputFormat.addDirs(job, sourceFile);
        }
    }
    
}
