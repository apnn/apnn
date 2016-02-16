package PanEval;

import io.github.htools.io.Datafile;
import io.github.htools.io.HPath;
import io.github.htools.lib.ArgsParser;
import io.github.htools.lib.Log;
import static io.github.htools.lib.PrintTools.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;

/**
 *
 * @author Jeroen
 */
public class EvalOut {
    public static Log log = new Log(EvalOut.class);

    public EvalOut(HPath results) throws InterruptedException, IOException {
        ArrayList<Datafile> files = results.getFiles();
        Collections.sort(files);
        for (Datafile df : files) {
            HashMap<String, Double> eval = read(df);
            printf("%s & %1.04f & %1.04f & %1.04f \\\\\n", df.getName(),
                    eval.get("Recall"), eval.get("Precision"), eval.get("Plagdet"));
        }
    }
    
    public HashMap<String, Double> read(Datafile df) throws InterruptedException, IOException {
        HashMap<String, Double> map = new HashMap();
        for (String line : df.readLines()) {
            String[] parts = line.split("\\s+");
            map.put(parts[0], Double.parseDouble(parts[1]));
        }
        return map;
    }
    
    public static void main(String[] args) throws InterruptedException, IOException {
        ArgsParser ap = new ArgsParser(args, "results");
        HPath results = ap.getPath("results");
        new EvalOut(results);
    }
}
