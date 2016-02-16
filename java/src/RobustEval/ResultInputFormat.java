package RobustEval;

import io.github.htools.hadoop.io.StructuredFileInputFormat;

public class ResultInputFormat extends StructuredFileInputFormat<ResultFile, ResultWritable> {

    public ResultInputFormat() {
        super(ResultFile.class);
    }
}
