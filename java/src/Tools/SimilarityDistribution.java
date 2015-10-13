package Tools;

import SimilarityFile.SimilarityFile;
import SimilarityFile.SimilarityWritable;
import io.github.htools.collection.HashMapList;
import io.github.htools.hadoop.Conf;
import io.github.htools.io.Datafile;
import io.github.htools.lib.ArgsParser;
import io.github.htools.lib.DoubleTools;
import io.github.htools.lib.Log;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;

/**
 *
 * @author Jeroen
 */
public class SimilarityDistribution {

    public static Log log = new Log(SimilarityDistribution.class);

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
            ArrayList<Double> list = entry.getValue();
            for (int position = 0; position < list.size(); position++) {
                ArrayList<Double> values;
                if (position >= positions.size()) {
                    values = new ArrayList();
                    positions.add(values);
                } else {
                    values = positions.get(position);
                }
                values.add(list.get(position));
            }
        }
        for (int i = 0; i < positions.size(); i++) {
            double[] quartiles = DoubleTools.quartiles(positions.get(i));
            log.printf("%d %f %f %f %f %f", i, quartiles[2], quartiles[3], quartiles[1], 
                    quartiles[4], quartiles[0]);
        }
    }
}
