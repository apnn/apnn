package AnalyzeCosineSimilarity;

import TestGenericMR.TestGenericJob;
import io.github.htools.hadoop.Conf;
import io.github.htools.hadoop.Job;
import io.github.htools.lib.Log;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

import java.io.IOException;

/**
 * Analyzing the contribution of the top-5 tdidf terms to the total cosine similarity
 * between a document and its nearest neighbor.
 *
 * @author Jeroen
 */
public class AnalyzeJob {

    private static final Log log = new Log(AnalyzeJob.class);

    public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException {
        Conf conf = new Conf(args, "input output");
        conf.setMapMemoryMB(4096);
        conf.setTaskTimeout(30000000);
        conf.setMapSpeculativeExecution(false);
        conf.setSortMB(1000);
        Job job = new Job(conf);
        TestGenericJob.setupInputFormat(job, conf.get("input"), conf.get("output"));
        job.setMapperClass(AnalyzeMap.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Result.class);
        job.setNumReduceTasks(1);
        job.setReducerClass(AnalyzeReduce.class);
        job.setOutputFormatClass(NullOutputFormat.class);
        job.waitForCompletion(true);
    }
}
