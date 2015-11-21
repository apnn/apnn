package SimilarityFile;

import io.github.htools.io.Datafile;
import io.github.htools.hadoop.tsv.File;

/**
 *
 * @author jeroen
 */
public class SimilarityFile extends File<SimilarityWritable> {

    // id of a suspicious document
    public StringField id = addString("id");
    // id of a source document compared with
    public StringField source = this.addString("source");
    // similarity between the suspicious and the source document
    public DoubleField similarity = this.addDouble("similarity");
    public DoubleField indexsimilarity = this.addDouble("indexsimilarity");

    public SimilarityFile(Datafile df) {
        super(df);
    }

    @Override
    public SimilarityWritable newRecord() {
        return new SimilarityWritable();
    }  
}
