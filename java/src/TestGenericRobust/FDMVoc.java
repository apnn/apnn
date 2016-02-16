package TestGenericRobust;

import io.github.htools.collection.HashMapInt;
import io.github.htools.io.Datafile;
import io.github.htools.io.HDFSPath;
import io.github.htools.lib.Log;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;

/**
 * @author Jeroen
 */
public class FDMVoc {

    public static Log log = new Log(FDMVoc.class);
    Datafile df;
    String query;
    double collectionsize = 268184766;

    public FDMVoc(Configuration conf, String query) {
        this.query = query;
        String fdmvocfilename = conf.get("fdmvoc");
        df = new HDFSPath(conf, fdmvocfilename).getFile(query);
    }

    public static FDMVoc read(Configuration conf, String query) {
        FDMVoc voc = new FDMVoc(conf, query);
        if (voc.df.existsFile()) {
            for (String line : voc.df.readLines()) {
                String[] part = line.split("\t");
                String type = part[0];
                long key = Long.parseLong(part[1]);
                int frequency = Integer.parseInt(part[2]);
                switch (type) {
                    case "0":
                        voc.termFrequency.put(key, frequency);
                        break;
                    case "1":
                        voc.orderedFrequency.put(key, frequency);
                        break;
                    case "2":
                        voc.unorderedFrequency.put(key, frequency);
                        break;
                }
            }
        }
        return voc;
    }

        HashMapInt<Long> termFrequency = new HashMapInt();
        HashMapInt<Long> orderedFrequency = new HashMapInt();
        HashMapInt<Long> unorderedFrequency = new HashMapInt();

        public void addVoc(FDMDoc d) {
            termFrequency.add(d.independent);
            orderedFrequency.add(d.ordered);
            unorderedFrequency.add(d.unordered);
        }

        public void save() {
            df.openWrite();
            for (Map.Entry<Long, Integer> entry : termFrequency.entrySet()) {
                df.printf("%d %d %d\n", 0, entry.getKey(), entry.getValue());
            }
            for (Map.Entry<Long, Integer> entry : orderedFrequency.entrySet()) {
                df.printf("%d %d %d\n", 1, entry.getKey(), entry.getValue());
            }
            for (Map.Entry<Long, Integer> entry : unorderedFrequency.entrySet()) {
                df.printf("%d %d %d\n", 2, entry.getKey(), entry.getValue());
            }
            df.closeWrite();
        }

        public double getCollectionsize() {
            return collectionsize;
        }
}
