package Vocabulary;

import io.github.htools.collection.ArrayMap;
import io.github.htools.io.Datafile;
import io.github.htools.lib.Log;
import java.util.HashMap;
import java.util.Map;

/**
 *
 * @author Jeroen
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
