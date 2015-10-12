package SimilarityFile;

import io.github.htools.hadoop.Job;
import io.github.htools.hadoop.io.OutputFormat;
/**
 *
 * @author jeroen
 */
public class SimilarityOutputFormat extends OutputFormat<SimilarityFile, SimilarityWritable> {

    public SimilarityOutputFormat() {
        super(SimilarityFile.class, SimilarityWritable.class);
    }

    public SimilarityOutputFormat(Job job) {
        super(job, SimilarityFile.class, SimilarityWritable.class);
    }

}
