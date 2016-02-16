package TrecTopics;

import io.github.htools.io.Datafile;
import io.github.htools.lib.Log;
import io.github.htools.search.ByteSearch;
import io.github.htools.search.ByteSearchSection;
import io.github.htools.search.ByteSection;

/**
 * Extract the 250 TREC Robust 2004 topics from the original topics files and store these
 * in a document archive file so that processing is similar to processing documents from
 * the corpus.
 * @author Jeroen
 */
public class TopicsToDocuments {
    public static Log log = new Log(TopicsToDocuments.class);
    static ByteSection topic = ByteSection.create("<top>", "</top>");
    static ByteSection num = ByteSection.create("<num>\\s*Number:", "<");
    static ByteSection desc = ByteSection.create("<desc>", "<");
    static ByteSearch desc2 = ByteSearch.create("\\s*Description\\s*:");
    static ByteSearch num2 = ByteSearch.create("\\s*Number\\s*:");

    public static void main(String[] args) {
        Datafile out = new Datafile(args[1]);
        out.openWrite();
        Datafile df = new Datafile(args[0]);
        byte[] readFully = df.readFully();
        for (ByteSearchSection topicsection : topic.findAllSections(readFully)) {
            String number = getNumber(topicsection);
            String description = getDescription(topicsection);
            out.printf("<doc><docno>%s</docno><text>%s</text></doc>\n", number, description);
        }
        out.closeWrite();
    }
    
    public static String getDescription(ByteSearchSection section) {
        String description = desc.extractTrim(section);
        if (desc2.match(description)) {
            description = description.substring(description.indexOf(':')+1).trim();
        }
        return description;
    }
    
    public static String getNumber(ByteSearchSection section) {
        String number = num.extractTrim(section);
        if (num2.match(number)) {
            number = number.substring(number.indexOf(':')+1).trim();
        }
        return number;
    }
}
