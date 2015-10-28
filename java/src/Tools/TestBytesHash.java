package Tools;

import io.github.htools.lib.Log;
import io.github.htools.lib.MathTools;

/**
 *
 * @author Jeroen
 */
public class TestBytesHash {
    public static Log log = new Log(TestBytesHash.class);

    public static void main(String[] args) {
        byte[] a = "aap".getBytes();
        byte[] b = "aap noot".getBytes();
        log.info("%d", MathTools.hashCode(b, 0, b.length));
        log.info("%d", MathTools.hashCode(b, 1, a.length));
    }
}
