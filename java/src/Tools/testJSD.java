package Tools;

import io.github.htools.lib.ArrayTools;
import io.github.htools.lib.DoubleTools;
import io.github.htools.lib.Log;

/**
 *
 * @author Jeroen
 */
public class testJSD {
    public static Log log = new Log(testJSD.class);

    public static void main(String[] args) {
        //double p[] = { 0.00029421, 0.42837957, 0.1371827, 0.00029419, 0.00029419,
      // 0.40526004, 0.02741252, 0.00029422, 0.00029417, 0.00029418 };
        double p[] = { 0.5, 0.5, 0.0, 0.0, 0.0,
               0.0, 0.0, 0.0, 0.0, 0.0 };

        //double q[] = { 0.00476199, 0.004762, 0.004762, 0.00476202, 0.95714168,
        double q[] = { 0.0, 0.0, 0.2, 0.2, 0.1,
       0.1, 0.1, 0.1, 0.1, 0.1};
        log.info("%s", ArrayTools.toString(DoubleTools.average(p, q)));
        double JensenShannonDivergence = DoubleTools.JensenShannonDivergence(p, q);
        log.printf("%s", JensenShannonDivergence);
                
    }
    
}
