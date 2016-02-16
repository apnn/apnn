package PanEval;

import SimilarityFile.SimilarityFile;
import SimilarityFile.SimilarityWritable;
import edu.emory.mathcs.backport.java.util.Collections;
import io.github.htools.collection.HashMapList;
import io.github.htools.collection.HashMapSet;
import io.github.htools.hadoop.Conf;
import io.github.htools.io.Datafile;
import io.github.htools.io.HPath;
import io.github.htools.lib.Log;
import static io.github.htools.lib.PrintTools.sprintf;
import io.github.htools.search.ByteSearch;
import io.github.htools.search.ByteSearchPosition;
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
public class PanEval {

    public static final Log log = new Log(PanEval.class);
    ByteSearch plagiarism = ByteSearch.create("<feature.*?source_reference=\"source-document");
    ByteSearch number = ByteSearch.create("\\d+");
    HashMapList<Integer, String> mapRefined = new HashMapList();
    HPath refined;

    public PanEval(HPath input, HPath refined, HPath output) {
        this.refined = refined;

        HashSet<String> existingDirs = new HashSet();
        for (HPath p : output.getDirs()) {
            existingDirs.add(p.getName());
        }
        log.info("existing %s", existingDirs);
        log.info("input %s output %s", input.getCanonicalPath(), output.getCanonicalPath());
        ArrayList<Datafile> files = new ArrayList();
        for (Datafile df : input.getFiles()) {
            if (!existingDirs.contains(df.getName())) {
                files.add(df);
            }
        }
        // query, run, source
        HashMap<String, HPath> outPaths = new HashMap();
        HashMapList<Integer, HashMapSet<String, Integer>> queries = new HashMapList();
        for (Datafile df : files) {
            HashMapList<String, String> resultMap = getResults(df);
            for (Map.Entry<String, ArrayList<String>> querySource : resultMap.entrySet()) {
                HashMapSet<String, Integer> runSource = new HashMapSet();
                for (String s : querySource.getValue()) {
                    runSource.add(df.getName(), Integer.parseInt(s));
                }
                queries.add(Integer.parseInt(querySource.getKey()), runSource);
            }
            HPath outPath = output.getSubdir(df.getName());
            outPaths.put(df.getName(), outPath);
        }
        log.info("outPaths %s", outPaths);
        ArrayList<Integer> querynrs = new ArrayList(queries.keySet());
        Collections.sort(querynrs);
        for (int query : querynrs) {
            log.info("query %d", query);
            ArrayList<String> lines = get(query);
            if (lines.size() > 0) {
                for (HashMapSet<String, Integer> runs : queries.get(query)) {
                    for (String runname : runs.keySet()) {
                        HashSet<Integer> sources = runs.get(runname);
                        boolean begin = false;
                        ArrayList<String> outLines = new ArrayList();
                        for (String line : lines) {
                            ByteSearchPosition pos = plagiarism.findPos(line);
                            if (pos.found()) {
                                String sourceString = number.extract(pos.haystack, pos.end);
                                int sourceId = Integer.parseInt(sourceString);
                                if (sources.contains(sourceId)) {
                                    outLines.add(line);
                                    begin = true;
                                }
                            } else {
                                outLines.add(line);
                            }
                        }
                        if (begin) {
                            HPath path = outPaths.get(runname);
                            log.info("run %s pah %s", runname, path.getCanonicalPath());
                            Datafile dfOut = path.getFile(getName(query));
                            dfOut.openWrite();
                            for (String line : outLines) {
                                dfOut.printf("%s\n", line);
                            }
                            dfOut.closeWrite();
                        }
                    }
                }
            }
        }
    }

    public String getName(int query) {
        return sprintf("suspicious-document%05d.xml", query);
    }

    public ArrayList<String> get(int query) {
        ArrayList<String> result = mapRefined.get(query);
        if (result == null) {
            Datafile df = refined.getFile(getName(query));
            if (df.existsFile()) {
                result = df.readAsLineList();
            } else {
                result = new ArrayList();
            }
            mapRefined.put(query, result);
        }
        return result;
    }

    public HashMapList<String, String> getResults(Datafile results) {
        log.info("reading %s", results.getName());
        HashMapList<String, String> map = new HashMapList();
        for (String line : results.readLines()) {
            String[] parts = line.split("\t+");
            map.add(parts[0], parts[1]);
        }
        return map;
    }

    public static void main(String[] args) {
        Conf conf = new Conf(args, "results refined output");
        new PanEval(conf.getFSPath("results"), conf.getFSPath("refined"), conf.getFSPath("output"));
    }
}
