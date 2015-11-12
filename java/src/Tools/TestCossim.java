package Tools;

import TestGeneric.Tokenizer;
import TestGeneric.TokenizerRemoveStopwords;
import io.github.htools.io.Datafile;
import io.github.htools.lib.Log;
import io.github.htools.type.TermVectorDouble;
import io.github.htools.type.TermVectorInt;

/**
 *
 * @author Jeroen
 */
public class TestCossim {
    public static Log log = new Log(TestCossim.class);

    public static void main(String[] args) {
        Tokenizer t = new TokenizerRemoveStopwords();
        Datafile f1 = new Datafile(args[0]);
        Datafile f2 = new Datafile(args[1]);
        String t1 = f1.readAsString();
        String t2 = f2.readAsString();
        TermVectorDouble d = new TermVectorDouble(t.tokenize(t1));
        TermVectorDouble d2 = new TermVectorDouble(t.tokenize(t2));
        log.info("%f", d.cossim(d2));
    }
    
}
