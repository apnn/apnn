package NYTEval;

import io.github.htools.hadoop.tsv.Writable;
import io.github.htools.io.buffer.BufferDelayedWriter;
import io.github.htools.io.buffer.BufferReaderWriter;
import io.github.htools.lib.MathTools;

/**
 * A sentence from the (KBA) collection.
 *
 * @author jeroen
 */
public class ResultWritable extends Writable<ResultFile> {

    // id of a suspicious document
    public String run;
    // the similarity of the source document to the suspicious document
    public int queryid;
    // id of the source document
    public double recall;
    public double precision;

    public ResultWritable() {
    }

    public ResultWritable clone() {
        ResultWritable s = new ResultWritable();
        s.run = run;
        s.queryid = queryid;
        s.recall = recall;
        s.precision = precision;
        return s;
    }

    @Override
    public int hashCode() {
        return MathTools.hashCode(run.hashCode(), queryid);
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof ResultWritable) {
            ResultWritable oo = (ResultWritable) o;
            return oo.queryid == queryid && oo.run.equals(run);
        }
        return false;
    }

    @Override
    public void read(ResultFile f) {
        this.queryid = f.queryid.get();
        this.run = f.run.get();
        this.recall = f.recall.get();
        this.precision = f.precision.get();
    }

    @Override
    public void write(BufferDelayedWriter writer) {
        writer.write(run);
        writer.write(queryid);
        writer.write(recall);
        writer.write(precision);
    }

    @Override
    public void readFields(BufferReaderWriter reader) {
        run = reader.readString();
        queryid = reader.readInt();
        recall = reader.readDouble();
        precision = reader.readDouble();
    }

    @Override
    public void write(ResultFile file) {
        file.run.set(run);
        file.queryid.set(queryid);
        file.recall.set(recall);
        file.precision.set(precision);
        file.write();
    }

    
}
