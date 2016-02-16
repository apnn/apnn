package RobustEval;

import io.github.htools.hadoop.Job;
import io.github.htools.hadoop.io.OutputFormat;
/**
 *
 * @author jeroen
 */
public class ResultOutputFormat extends OutputFormat<ResultFile, ResultWritable> {

    public ResultOutputFormat() {
        super(ResultFile.class, ResultWritable.class);
    }

    public ResultOutputFormat(Job job) {
        super(job, ResultFile.class, ResultWritable.class);
    }

}
