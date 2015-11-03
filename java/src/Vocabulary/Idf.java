package Vocabulary;

import static Vocabulary.VocabularyMap.COLLECTIONSIZE;
import io.github.htools.collection.ArrayMap;
import io.github.htools.collection.HashMapDouble;
import io.github.htools.io.Datafile;
import io.github.htools.lib.Log;
import java.util.Map;

/**
 *
 * @author Jeroen
 */
public class Idf extends HashMapDouble<String> {

    public static Log log = new Log(Idf.class);
    private static String UNKNOWN = "###UNKNOWN###";
    private Double unknown;
    private long collectionsize;

    public Idf(Datafile vocabularyFile) {
        super(readIDF(vocabularyFile));
        collectionsize = Math.round(remove(COLLECTIONSIZE));
        unknown = idf(0, collectionsize);
    }

    @Override
    public Double get(Object key) {
        Double value = super.get(key);
        return value==null?unknown:value;
    }
    
    public static Map<String, Double> readIDF(Datafile df) {
        ArrayMap<String, Double> termIdf = new ArrayMap();
        VocabularyFile vocabularyFile = new VocabularyFile(df);
        vocabularyFile.setBufferSize(1000000);
        long collectionsize = 0;
        for (VocabularyWritable line : vocabularyFile) {
            if (line.documentFrequency > 0) {
                if (line.term.equals(COLLECTIONSIZE)) {
                    collectionsize = line.documentFrequency;
                    termIdf.add(COLLECTIONSIZE, (double) line.documentFrequency);
                    for (Map.Entry<String, Double> entry : termIdf) {
                        entry.setValue(idf(entry.getValue(), collectionsize));
                    }
                } else {
                    if (collectionsize == 0) {
                        termIdf.add(line.term, (double) line.documentFrequency);
                    } else {
                        termIdf.add(line.term, idf(line.documentFrequency, collectionsize));
                    }
                }
            }
        }
        return termIdf;
    }

    public static double idf(double df, long collectionsize) {
        return Math.log((collectionsize + 1) / (df + 1));
    }
}
