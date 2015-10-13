package Tools;

import SimilarityFile.SimilarityFile;
import SimilarityFile.SimilarityWritable;
import io.github.htools.collection.HashMapList;
import io.github.htools.hadoop.Conf;
import io.github.htools.io.Datafile;
import io.github.htools.lib.ArgsParser;
import io.github.htools.lib.ArrayTools;
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
public class SimilarityDistribution3 {

    public static Log log = new Log(SimilarityDistribution3.class);

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
            double[] list = ArrayTools.toDoubleArray(entry.getValue());
            for (int i = 0; i < list.length; i++) {
                double mean = DoubleTools.mean(list, i+1, list.length);
                double sd = DoubleTools.standardDeviation(list, i+1, list.length, mean);
                double p = MathTools.cumulativeProbability(list[i], mean, sd);
                if (positions.size() <= i) {
                    ArrayList<Double> position = new ArrayList();
                    positions.add(position);
                    position.add(p);
                } else {
                    ArrayList<Double> position = positions.get(i);
                    position.add(p);
                }
            }
        }
        for (int i = 0; i < positions.size(); i++) {
            log.printf("%d %.20f", i, DoubleTools.mean(positions.get(i)));
        }
    }
}
