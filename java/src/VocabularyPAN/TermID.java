package VocabularyPAN;

import io.github.htools.collection.ArrayMap;
import io.github.htools.io.Datafile;
import io.github.htools.lib.Log;
import java.util.HashMap;
import java.util.Map;

/**
 * reads a vocabulary file, and maps terms to termids. A termid is equal to the line
 * number the term is at, if in a vocabulary file #DOCS is often at line 0 and
 * "the" is on line 1, the then the termid for "the" = 1.
 */
public class TermID extends HashMap<String, Integer> {

    public static Log log = new Log(TermID.class);

    public TermID(Datafile vocabularyFile) {
        super(readTerms(vocabularyFile));
    }
    
    public static Map<String, Integer> readTerms(Datafile df) {
        ArrayMap<String, Integer> termIdMap = new ArrayMap();
        VocabularyFile vocabularyFile = new VocabularyFile(df);
        vocabularyFile.setBufferSize(1000000);
        for (VocabularyWritable line : vocabularyFile) {
            termIdMap.add(line.term, termIdMap.size());
        }
        return termIdMap;
    }
    
    public static Map<Integer, String> readIdToTerm(Datafile df) {
        ArrayMap<Integer, String> termIdMap = new ArrayMap();
        VocabularyFile vocabularyFile = new VocabularyFile(df);
        vocabularyFile.setBufferSize(1000000);
        for (VocabularyWritable line : vocabularyFile) {
            termIdMap.add(termIdMap.size(), line.term);
        }
        return termIdMap;
    }
}
