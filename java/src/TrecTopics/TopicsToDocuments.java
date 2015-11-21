package TrecTopics;

import io.github.htools.io.Datafile;
import io.github.htools.lib.Log;
import io.github.htools.search.ByteSearchSection;
import io.github.htools.search.ByteSection;

/**
 *
 * @author Jeroen
 */
public class TopicsToDocuments {
    public static Log log = new Log(TopicsToDocuments.class);
    static ByteSection topic = ByteSection.create("<top>", "</top>");
    static ByteSection num = ByteSection.create("<num>\\s*Number:", "<");
    static ByteSection desc = ByteSection.create("<desc>\\s*Description:", "<");

    public static void main(String[] args) {
        Datafile out = new Datafile(args[1]);
        out.openWrite();
        Datafile df = new Datafile(args[0]);
        byte[] readFully = df.readFully();
        for (ByteSearchSection topicsection : topic.findAllSections(readFully)) {
            String number = num.extractTrim(topicsection);
            String description = desc.extractTrim(topicsection);
            out.printf("<doc><docno>%s</docno><text>%s</text></doc>\n", number, description);
        }
        out.closeWrite();
    }
    
}
