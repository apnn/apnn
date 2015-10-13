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
public class SimilarityDistribution1 {

    public static Log log = new Log(SimilarityDistribution1.class);

    public static void main(String[] args) throws IOException {
        Conf conf = new Conf(args, "input");
        HashMapList<Integer, Double> map = new HashMapList();
        Datafile file = conf.getHDFSFile("input");
        SimilarityFile similarityFile = new SimilarityFile(file);
        for (SimilarityWritable s : similarityFile) {
            map.add(s.id, s.score);
        }
        ArrayList<ArrayList<Double>> positions = new ArrayList();
        for (Map.Entry<Integer, ArrayList<Double>> entry : map.entrySet()) {
            ArrayList<Double> second = entry.getValue();
            ArrayList<Double> first = new ArrayList();
            while (second.size() > 1 && first.size() < 100) {
                first.add(second.remove(0));
                double ttest = DoubleTools.welchTTestOneSided(first, second);
                if (positions.size() < first.size()) {
                    ArrayList<Double> position = new ArrayList();
                    positions.add(position);
                    position.add(ttest);
                } else {
                    ArrayList<Double> position = positions.get(first.size()-1);
                    position.add(ttest);
                }
            }
        }
        for (int i = 0; i < positions.size(); i++) {
            log.printf("%d %.20f", i, DoubleTools.mean(positions.get(i)));
        }
    }
}
