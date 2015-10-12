package SimilarityFile;

import io.github.htools.io.buffer.BufferDelayedWriter;
import io.github.htools.io.buffer.BufferReaderWriter;
import io.github.htools.lib.MathTools;
import io.github.htools.hadoop.tsv.Writable;
import java.io.IOException;

/**
 * A sentence from the (KBA) collection.
 * @author jeroen
 */
public class SimilarityWritable extends Writable<SimilarityFile> {
    // id of a suspicious document
    public int id;
    // the similarity of the source document to the suspicious document
    public double score;
    // id of the source document
    public int source;

    public SimilarityWritable() {
    }

    public SimilarityWritable clone() {
        SimilarityWritable s = new SimilarityWritable();
        s.id = id;
        s.source = source;
        s.score = score;
        return s;
    }
    
    @Override
    public int hashCode() {
        return MathTools.hashCode(id, source);
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof SimilarityWritable) {
           SimilarityWritable oo = (SimilarityWritable) o;
           return oo.id == id && oo.source == source;
        }
        return false;
    }

    @Override
    public void read(SimilarityFile f) {
        this.id = f.id.get();
        this.source = f.source.get();
        this.score = f.similarity.get();
    }

    @Override
    public void write(BufferDelayedWriter writer)  {
        writer.write(id);
        writer.write(source);
        writer.write(score);
    }

    @Override
    public void readFields(BufferReaderWriter reader) {
        id = reader.readInt();
        source = reader.readInt();
        score = reader.readDouble();
    }

    @Override
    public void write(SimilarityFile file) throws IOException {
        file.id.set(id);
        file.source.set(source);
        file.similarity.set(score);
        file.write();
    }
}
