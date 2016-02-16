package TestGenericRobust;

import io.github.htools.lib.Log;
import io.github.htools.hadoop.Conf;

/**
 *
 * @author Jeroen
 */
public class BFDLM {

    private static final Log log = new Log(BFDLM.class);

    public static void main(String[] args) throws Exception {

        Conf conf = new Conf(args, "sourcepath queries output fdmvoc");
        conf.setTaskTimeout(1800000);
        
        BfFDMJob job = new BfFDMJob(conf,
                conf.get("sourcepath"),
                conf.get("queries"),
                conf.get("output"),
                conf.get("fdmvoc"));

        job.setReducerClass(BfDLMReduce.class);
        job.waitForCompletion(true);
    }
}
