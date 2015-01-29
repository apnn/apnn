package kba4sort;

import io.github.repir.tools.io.HDFSPath;
import io.github.repir.tools.lib.Log;
import io.github.repir.tools.hadoop.Conf;
import io.github.repir.tools.hadoop.io.DayPartitioner;
import io.github.repir.tools.hadoop.Job;
import java.io.IOException;
import java.text.ParseException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import streamcorpus.sentence.SentenceInputFormat;
import streamcorpus.sentence.SentenceWritable;
/**
 *
 * @author jeroen
 */
public class SortJob {
   public static final Log log = new Log( SortJob.class );

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException, ParseException {
        Conf conf = new Conf(args, "-i input -o output -s startdate -e enddate");
        conf.setMapMemoryMB(4096);
        conf.setReduceSpeculativeExecution(false);

        DayPartitioner.setTime(conf, conf.get("startdate"), conf.get("enddate"));
        
        String input = conf.get("input");        
        Job job = new Job(conf, input, conf.get("output"), conf.get("startdate"), conf.get("enddate"));
        
        int reducers = DayPartitioner.getNumberOfReducers(conf);
        log.info("reducers %d", reducers);
        job.setNumReduceTasks(reducers);
        job.setInputFormatClass(SentenceInputFormat.class);
        SentenceInputFormat.addDirs(job, input);
        
        job.setMapperClass(SortMap.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(SentenceWritable.class);
        
        job.setSortComparatorClass(LongWritable.Comparator.class);
        job.setPartitionerClass(DayPartitioner.class);
        job.setReducerClass(SortReducer.class);
        
        Path out = new Path(conf.get("output"));
        job.setOutputFormatClass(NullOutputFormat.class);
        new HDFSPath(conf, out).trash();
        
        job.waitForCompletion(true);
    }
   
}
