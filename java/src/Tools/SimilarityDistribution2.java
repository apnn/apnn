package Tools;

import SimilarityFile.SimilarityFile;
import SimilarityFile.SimilarityWritable;
import io.github.htools.collection.HashMapList;
import io.github.htools.hadoop.Conf;
import io.github.htools.io.Datafile;
import io.github.htools.lib.ArgsParser;
import io.github.htools.lib.DoubleTools;
import io.github.htools.lib.IntTools;
import io.github.htools.lib.Log;
import io.github.htools.lib.MathTools;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;

/**
 *
 * @author Jeroen
 */
public class SimilarityDistribution2 {

    public static Log log = new Log(SimilarityDistribution2.class);

    public static void main(String[] args) throws IOException {
        Conf conf = new Conf(args, "input");
        HashMapList<Integer, Double> map = new HashMapList();
        Datafile file = conf.getHDFSFile("input");
        SimilarityFile similarityFile = new SimilarityFile(file);
        for (SimilarityWritable s : similarityFile) {
            map.add(s.id, s.score);
        }
        ArrayList<ArrayList<Double>> psecond = new ArrayList();
        ArrayList<ArrayList<Double>> pfirst = new ArrayList();
        for (Map.Entry<Integer, ArrayList<Double>> entry : map.entrySet()) {
            ArrayList<Double> third = entry.getValue();
            ArrayList<Double> second = new ArrayList();
            ArrayList<Double> first = new ArrayList();
            second.add(third.remove(0));
            second.add(third.remove(0));
            do {
                double ttest = DoubleTools.welchTTestOneSided(second, third);
                if (psecond.size() <= first.size()) {
                    ArrayList<Double> position = new ArrayList();
                    psecond.add(position);
                    position.add(ttest);
                    position = new ArrayList();
                    pfirst.add(position);
                } else {
                    ArrayList<Double> position = psecond.get(first.size());
                    position.add(ttest);
                }
                if (first.size() > 1) {
                    double ttestf = DoubleTools.welchTTestOneSided(first, second);
                    ArrayList<Double> position = pfirst.get(first.size());
                    position.add(ttestf);
                }
                first.add(second.remove(0));
                second.add(third.remove(0));
            } while (third.size() > 1);
        }
        for (int i = 0; i < pfirst.size(); i++) {
            log.printf("%d %.20f %.20f", i, DoubleTools.mean(pfirst.get(i)), DoubleTools.mean(psecond.get(i)));
        }
    }
}
