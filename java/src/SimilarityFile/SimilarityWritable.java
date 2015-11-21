package SimilarityFile;

import TestGeneric.Candidate;
import io.github.htools.io.buffer.BufferDelayedWriter;
import io.github.htools.io.buffer.BufferReaderWriter;
import io.github.htools.lib.MathTools;
import io.github.htools.hadoop.tsv.Writable;
import java.io.IOException;
import java.util.Comparator;

/**
 * A sentence from the (KBA) collection.
 *
 * @author jeroen
 */
public class SimilarityWritable extends Writable<SimilarityFile> {

    // id of a suspicious document
    public String id;
    // the similarity of the source document to the suspicious document
    public double measureSimilarity;
    // id of the source document
    public String source;
    public double indexSimilarity;

    public SimilarityWritable() {
    }

    public SimilarityWritable clone() {
        SimilarityWritable s = new SimilarityWritable();
        s.id = id;
        s.source = source;
        s.measureSimilarity = measureSimilarity;
        s.indexSimilarity = indexSimilarity;
        return s;
    }

    @Override
    public int hashCode() {
        return MathTools.hashCode(id.hashCode(), source.hashCode());
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof SimilarityWritable) {
            SimilarityWritable oo = (SimilarityWritable) o;
            return oo.id.equals(id) && oo.source.equals(source);
        }
        return false;
    }

    @Override
    public void read(SimilarityFile f) {
        this.id = f.id.get();
        this.source = f.source.get();
        this.measureSimilarity = f.similarity.get();
        this.indexSimilarity = f.indexsimilarity.get();
    }

    @Override
    public void write(BufferDelayedWriter writer) {
        writer.write(id);
        writer.write(source);
        writer.write(measureSimilarity);
        writer.write(indexSimilarity);
    }

    @Override
    public void readFields(BufferReaderWriter reader) {
        id = reader.readString();
        source = reader.readString();
        measureSimilarity = reader.readDouble();
        indexSimilarity = reader.readDouble();
    }

    @Override
    public void write(SimilarityFile file) {
        file.id.set(id);
        file.source.set(source);
        file.similarity.set(measureSimilarity);
        file.indexsimilarity.set(indexSimilarity);
        file.write();
    }

    
}
