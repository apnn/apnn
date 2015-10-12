package Tools;

import TestGeneric.Document;
import io.github.htools.hadoop.Conf;
import io.github.htools.io.Datafile;
import io.github.htools.lib.Log;
import java.io.IOException;

/**
 * gives the cosine similarity between two documents
 * @author Jeroen
 */
public class TestCossimDocs {
    public static Log log = new Log(TestCossimDocs.class);

    public static void main(String[] args) throws IOException {
        Conf conf = new Conf(args, "a b");
        byte[] contentA = new Datafile(conf.get("a")).readFully();
        byte[] contentB = new Datafile(conf.get("b")).readFully();
        Document docA = new Document(0, contentA);
        Document docB = new Document(0, contentB);
        log.info("%f %f %f", docA.getModel().magnitude(), docB.getModel().magnitude(), docA.getModel().cossim(docB.getModel()));
    }
}
