package VocabularyPAN;

import io.github.htools.io.buffer.BufferDelayedWriter;
import io.github.htools.io.buffer.BufferReaderWriter;
import io.github.htools.hadoop.tsv.Writable;

/**
 * A term with the document en term frequency in the collection
 * @author jeroen
 */
public class VocabularyWritable extends Writable<VocabularyFile> {
    public String term;
    public int documentFrequency;
    public long termFrequency;

    public VocabularyWritable() {
    }

    @Override
    public void read(VocabularyFile f) {
        this.term = f.term.get();
        this.documentFrequency = f.documentFrequency.get();
        this.termFrequency = f.termFrequency.get();
    }

    @Override
    public void write(BufferDelayedWriter writer)  {
        writer.write(term);
        writer.write(documentFrequency);
        writer.write(termFrequency);
    }

    @Override
    public void readFields(BufferReaderWriter reader) {
        term = reader.readString();
        documentFrequency = reader.readInt();
        termFrequency = reader.readLong();
    }

    @Override
    public void write(VocabularyFile file) {
        file.term.set(term);
        file.documentFrequency.set(documentFrequency);
        file.termFrequency.set(termFrequency);
        file.write();
    }
}
