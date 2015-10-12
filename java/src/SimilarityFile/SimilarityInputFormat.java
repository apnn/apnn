package SimilarityFile;

import io.github.htools.hadoop.io.StructuredFileInputFormat;

public class SimilarityInputFormat extends StructuredFileInputFormat<SimilarityFile, SimilarityWritable> {

    public SimilarityInputFormat() {
        super(SimilarityFile.class);
    }
}
