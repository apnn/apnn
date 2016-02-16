package NYTEval;

import io.github.htools.hadoop.Conf;
import io.github.htools.io.Datafile;
import io.github.htools.io.HDFSPath;
import io.github.htools.io.HPath;
import io.github.htools.lib.Log;

/**
 * Extract the document frequency of terms from a Wikipedia XML source file
 * @author jeroen
 */

public class EvalReport {

    private static final Log log = new Log(EvalReport.class);

    public static void main(String[] args) throws Exception {
        Conf conf = new Conf(args, "-i input");
        HDFSPath in = conf.getHDFSPath("input");
        report(in);
    }

    public static void report(HPath in) {
        for (Datafile df : in.getFiles()) {
            String run = df.getName();
            double r[] = new double[4];
            int i = 0;
            log.info("file %s", df.getCanonicalPath());
            for (String line : df.readLines()) {
                String part[] = line.split("\t");
                r[i++] = Double.parseDouble(part[1]);
            }
            log.printf("%s\t& %.4f\t& %.4f\t& %.4f \\\\", run, r[0], r[1], r[3] );
        }
    }
}
