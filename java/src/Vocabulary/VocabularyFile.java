package Vocabulary;

import io.github.htools.collection.HashMapDouble;
import io.github.htools.io.Datafile;
import io.github.htools.hadoop.tsv.File;

/**
 * Container for the data of the vocabulary of a corpus with frequencies
 * @author jeroen
 */
public class VocabularyFile extends File<VocabularyWritable> {

    // term that appears in the collection
    public StringField term = addString("term");
    // number of documents in the collection in which the term appears
    public IntField documentFrequency = addInt("documentfrequency");
    // frequency of the term in the collection
    public IntField termFrequency = addInt("termfrequency");

    public VocabularyFile(Datafile df) {
        super(df);
    }

    @Override
    public VocabularyWritable newRecord() {
        return new VocabularyWritable();
    }
}
