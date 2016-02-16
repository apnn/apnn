package VocabularyPAN;

import io.github.htools.collection.ArrayMap;
import io.github.htools.io.Datafile;
import io.github.htools.lib.Log;
import java.util.HashMap;
import java.util.Map;

/**
 * reads a vocabulary file, and maps termids to terms. A termid is equal to the line
 * number the term is at, if in a vocabulary file #DOCS is often at line 0 and
 * "the" is on line 1, the then the termid for "the" = 1.
 */
public class IdTerm extends HashMap<Integer, String> {

    public static Log log = new Log(IdTerm.class);

    public IdTerm(Datafile vocabularyFile) {
        super(readIdToTerm(vocabularyFile));
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
