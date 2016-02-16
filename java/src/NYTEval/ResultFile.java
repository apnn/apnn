package NYTEval;

import io.github.htools.hadoop.tsv.File;
import io.github.htools.io.Datafile;

/**
 *
 * @author jeroen
 */
public class ResultFile extends File<ResultWritable> {

    // id of a suspicious document
    public StringField run = addString("run");
    // id of a source document compared with
    public IntField queryid = this.addInt("queryid");
    // similarity between the suspicious and the source document
    public DoubleField recall = this.addDouble("recall");
    public DoubleField precision = this.addDouble("precision");

    public ResultFile(Datafile df) {
        super(df);
    }

    @Override
    public ResultWritable newRecord() {
        return new ResultWritable();
    }  
}
