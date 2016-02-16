package PanDetection;

import PanDetection.AnnChunk.Passage;
import io.github.htools.hadoop.Conf;
import io.github.htools.hadoop.Job;
import io.github.htools.lib.Log;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

import java.io.IOException;

import static TestGenericMR.TestGenericJob.setupInputFormat;

/**
 * Extracts plagiarized passages from a collection of source documents for a set of
 * suspicious documents for the PAN11 collection, using a simplified implementation
 * of the algorithm proposed in Kasprzak and Brandejs: "Improving the reliability of
 * the plagiarism detection system." Lab Report for PAN at CLEF, pages 359–366, 2010.
 * The algorithm sorts every chunk of 5 words, and matches passages between source and
 * query documents that have at least 20 of those 5-word chunks in common, while the
 * maximum allowed gap between 2 chunks is no more than 100 words (the original paper
 * used 50 words).
 *
 * In the output folder, per suspicious document the detected passages are written
 * to an .xml file, with a format that conforms to the format used by the official
 * PAN11 evaluation tool.
 *
 * parameters:
 *
 * sources: HDFS path containing the PAN11 source documents wrapped in
 * ArchiveFiles (e.g. .tar.lz4) suspicious: HDFS path containing the PAN11
 * suspicious documents wrapped in ArchiveFiles (e.g. .tar.lz4) output: the
 * resulting k-most similar source documents per suspicious document are written
 * to a file with this name in SimilarityFile format
 *
 * @author Jeroen
 */
public class PanJob extends Job {

    private static final Log log = new Log(PanJob.class);

    public PanJob(Conf conf, String source, String query, String output, String vocabulary) throws IOException {
        super(conf);
        setupInputFormat(this, source, query);
        setMapperClass(PanMap.class);
        setMapOutputKeyClass(Text.class);
        setMapOutputValueClass(Passage.class);
        setNumReduceTasks(1);
        setReducerClass(PanReduce.class);
        setOutputFormatClass(NullOutputFormat.class);
        this.setJobName("PanJob chunk5");
    }
    
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Conf conf = new Conf(args, "source query output vocabulary");
        PanJob job = new PanJob(conf, conf.get("source"), conf.get("query"),
                                conf.get("output"), conf.get("vocabulary"));
        job.waitForCompletion(true);
    }
}
