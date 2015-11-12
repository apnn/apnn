package Tools;

import io.github.htools.io.Datafile;
import io.github.htools.lib.Log;
import io.github.htools.type.TermVectorDouble;

/**
 *
 * @author Jeroen
 */
public class TestCossimD {

    public static Log log = new Log(TestCossimD.class);

    public static void main(String[] args) {
        TermVectorDouble d = getDocumentAsVector(args[0]);
        TermVectorDouble d2 = getDocumentAsVector(args[1]);
        log.info("%f", d.cossim(d2));
    }

    public static TermVectorDouble getDocumentAsVector(String filename) {
        TermVectorDouble vector = new TermVectorDouble();
        Datafile f1 = new Datafile(filename);
        String t = f1.readAsString();
        for (String line : t.split("\n")) {
            if (line.trim().length() > 0) {
                String[] term = line.split("\\s");
                vector.add(term[0], Double.parseDouble(term[1]));
            }
        }
        return vector;
    }

}
