package RobustEval;

import edu.emory.mathcs.backport.java.util.Collections;
import io.github.htools.collection.HashMapList;
import io.github.htools.collection.TreeMapList;
import io.github.htools.hadoop.Conf;
import io.github.htools.io.Datafile;
import io.github.htools.io.HPath;
import io.github.htools.lib.Log;
import io.github.htools.search.ByteSearch;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

/**
 * reduces all scored similarities between suspicious documents (=key) and all
 * source documents, keeping only the k-most similar source documents per
 * suspicious document.
 *
 * @author jeroen
 */
public class RobustEval {

    public static final Log log = new Log(RobustEval.class);
    ByteSearch plagiarism = ByteSearch.create("<feature.*?source_reference=\"source-document");
    ByteSearch number = ByteSearch.create("\\d+");
    HashMapList<Integer, String> mapRefined = new HashMapList();
    HPath refined;

    public RobustEval(HPath results, HPath refined, HPath output, HPath outputTrec) {
        this.refined = refined;
        HashMap<String, HashMapList<Integer, Result>> resultMap = getResults(results, output);
//        for (Datafile df : refined.getFiles()) {
//            df.setBufferSize(2000000);
//            log.info("%s", df.getCanonicalPath());
//            HashMap<String, Double> fdm = getFDM(df);
//            int query = Integer.parseInt(df.getName());
//            for (HashMapList<Integer, Result> entry : resultMap.values()) {
//                ArrayList<Result> list = entry.getList(query);
//                for (int i = list.size() - 1; i >= 0; i--) {
//                    Result r = list.get(i);
//                    Double score = fdm.get(r.id);
//                    if (score == null) {
//                        list.remove(i);
//                    } else {
//                        r.fdmScore = fdm.get(r.id);
//                    }
//                }
//            }
//        }
        for (Map.Entry<String, HashMapList<Integer, Result>> entry : resultMap.entrySet()) {
            String system = entry.getKey();
            Datafile df = output.getFile(system);
            Datafile dfTrec = outputTrec.getFile(system);
            df.openWrite();
            dfTrec.openWrite();
            TreeMapList<Integer, Result> sorted = new TreeMapList(entry.getValue());
            for (Map.Entry<Integer, ArrayList<Result>> query : sorted.entrySet()) {
                Collections.sort(query.getValue());
                for (int i = 0; i < query.getValue().size(); i++) {
                    Result r = query.getValue().get(i);
                    printTrec(dfTrec, query.getKey(), i + 1, r);
                    print(df, query.getKey(), i + 1, r);
                }
            }
            df.closeWrite();
            dfTrec.closeWrite();
        }
    }
    
    public void printTrec(Datafile df, int query, int rank, Result result) {
        df.printf("%s Q0 %s %d %f %s\n", query, result.id, rank, result.fdmScore, "run");
    }

    public void print(Datafile df, int query, int rank, Result result) {
        df.printf("%d\t%s\t%f\t%f\n", query, result.id, result.fdmScore, result.fdmScore);
    }
    
    public HashSet<String> getExistingFiles(HPath resultsPath) {
        return new HashSet<String>(resultsPath.existsDir() ? resultsPath.getFilenames() : new ArrayList());
    }

    public HashMap<String, HashMapList<Integer, Result>> getResults(HPath resultPath, HPath output) {
        HashMap<String, HashMapList<Integer, Result>> result = new HashMap();
        HashSet<String> existingFiles = getExistingFiles(output);
        for (Datafile df : resultPath.getFiles()) {
            if (!existingFiles.contains(df.getName())) {
                df.setBufferSize(1000000);
                log.info("%s", df.getCanonicalPath());
                String system = df.getName();
                HashMapList<Integer, Result> retrieved = new HashMapList();
                for (String line : df.readLines()) {
                    String[] parts = line.split("\t");
                    if (parts.length > 2) {
                        Result doc = new Result();
                        doc.id = parts[1];
                        doc.fdmScore = Double.parseDouble(parts[2]);
                        int query = Integer.parseInt(parts[0]);
                        retrieved.add(query, doc);
                    }
                }
                result.put(system, retrieved);
            }
        }
        return result;
    }

    public HashMap<String, Double> getFDM(Datafile refined) {
        HashMap<String, Double> result = new HashMap();
        for (String line : refined.readLines()) {
            String[] parts = line.split("\t");
            if (parts.length > 0) {
                result.put(parts[1], Double.parseDouble(parts[2]));
            }
        }
        return result;
    }

    class Result implements Comparable<Result> {

        String id;
        double fdmScore;

        @Override
        public int compareTo(Result o) {
            return fdmScore > o.fdmScore ? -1 : fdmScore < o.fdmScore ? 1 : 0;
        }
    }

    public static void main(String[] args) {
        Conf conf = new Conf(args, "results refined output outputtrec");
        new RobustEval(conf.getFSPath("results"), conf.getFSPath("refined"),
                conf.getFSPath("output"), conf.getFSPath("outputtrec"));
    }
}
