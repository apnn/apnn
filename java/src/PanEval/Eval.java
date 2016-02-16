package PanEval;

import io.github.htools.io.Datafile;
import io.github.htools.io.HPath;
import io.github.htools.lib.ArgsParser;
import io.github.htools.lib.Log;
import io.github.htools.lib.ShellTools;

import java.io.IOException;
import java.util.*;

/**
 *
 * @author Jeroen
 */
public class Eval {
    public static Log log = new Log(Eval.class);

    public Eval(HPath results, HPath out) throws InterruptedException, IOException {
        ArrayList<? extends HPath> files = results.getDirs();
        HashSet<String> existing = new HashSet(out.getFilenames());
        log.info("%s %s", existing, files);
        for (HPath df : files) {
            if (!existing.contains(df.getName())) {
                log.info("%s", df.getName());
                HashMap<String, Double> eval = runEval(df);
                write(out.getFile(df.getName()), eval);
            }
        }
    }
    
   public void write(Datafile out, HashMap<String, Double> map) {
       out.openWrite();
       for (Map.Entry<String, Double> entry : map.entrySet()) {
           out.printf("%s\t%f\n", entry.getKey(), entry.getValue());
       }
       out.closeWrite();
   }
    
    public HashMap<String, Double> runEval(HPath df) throws InterruptedException, IOException {
        ArrayList<String> lines = ShellTools.executeCommand("pan11/perfmeasures.py -p pan11/suspicious-documents-gt -d %s", df.getCanonicalPath());
        HashMap<String, Double> results = new HashMap();
        for (String line : lines ) {
            String[] part = line.split("\\s+");
            switch (part[0]) {
                case "Plagdet":
                    results.put(part[0], Double.parseDouble(part[2]));
                    break;
                case "Recall":
                case "Precision":
                    results.put(part[0], Double.parseDouble(part[1]));
            }
        }
        return results;
    }
    
    public static void main(String[] args) throws InterruptedException, IOException {
        ArgsParser ap = new ArgsParser(args, "results output");
        HPath results = ap.getPath("results");
        HPath out = ap.getPath("output");
        new Eval(results, out);
    }
}
