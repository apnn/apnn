package NYTEval;

import io.github.htools.collection.HashMapSet;
import io.github.htools.hadoop.Conf;
import io.github.htools.hadoop.ContextTools;
import io.github.htools.io.Datafile;
import io.github.htools.lib.Log;
import io.github.htools.search.ByteSearch;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;

/**
 * reduces all scored similarities between suspicious documents (=key) and all
 * source documents, keeping only the k-most similar source documents per
 * suspicious document.
 *
 * @author jeroen
 */
public class RefineReduce extends Reducer<IntWritable, RefineMap.Result, NullWritable, ResultWritable> {

    public static final Log log = new Log(RefineReduce.class);
    Conf conf;
    ArrayList<String> ids;
    int key = -1;
    HashMapSet<Integer, String> labels;
    ResultWritable value = new ResultWritable();

    @Override
    public void setup(Context context) throws IOException {
        conf = ContextTools.getConfiguration(context);
        ids = readIds();
    }

    @Override
    public void reduce(IntWritable doc, Iterable<RefineMap.Result> results, Context context) throws IOException, InterruptedException {
        if (key != doc.get()) {
            key = doc.get();
            readSources(key);
        }
        for (RefineMap.Result result : results) {
            HashSet<String> ql = labels.get(result.queryid);
            if (ql == null) {
                log.info("null for run %s source %d query %d", result.run, result.sourceid, result.queryid);
            }

            HashSet<String> sl = labels.get(result.sourceid);
            if (sl == null) {
                log.info("null for run %s source %d query %d", result.run, result.sourceid, result.queryid);
            }
            int count = 0;
            for (String label : sl) {
                if (ql.contains(label))
                    count++;
            }
            value.recall = count / (double) ql.size();
            value.precision = count / (double) sl.size();
            value.queryid = result.queryid;
            value.run = result.run;
            context.write(NullWritable.get(), value);
        }
    }

    public void readSources(int key) {
        labels = new HashMapSet();
        int queryline = key / ids.size();
        int sourceline = key % ids.size();
        readLabels(queryline);
        if (sourceline != queryline)
            readLabels(sourceline);
    }

    public ArrayList<String> readIds() {
        ArrayList<String> ids = new ArrayList();
        ByteSearch filename = ByteSearch.create("[0-9\\.]+");
        for (String line : conf.getHDFSFile("ids").readLines()) {
            ids.add(filename.extract(line));
        }
        return ids;
    }

    public void readLabels(int id) {
        Datafile file = conf.getHDFSPath("annotation").getFile(ids.get(id));
        log.info("file %s", file.getCanonicalPath());
        for (String line : file.readLines()) {
            String[] part = line.split("\t");
            if (part.length < 2)
                log.info("line %s", line);
            labels.add(Integer.parseInt(part[0]), part[1]);
        }
    }


}
