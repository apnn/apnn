package Pan11GT;

import SimilarityFile.SimilarityFile;
import SimilarityFile.SimilarityWritable;
import io.github.htools.collection.ArrayMap;
import io.github.htools.collection.HashMapInt;
import io.github.htools.collection.HashMapList;
import io.github.htools.io.Datafile;
import io.github.htools.io.FSPath;
import io.github.htools.io.struct.XMLReader;
import io.github.htools.lib.ArgsParser;
import io.github.htools.lib.CollectionTools;
import io.github.htools.lib.Log;
import io.github.htools.search.ByteSearch;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import javax.xml.stream.XMLStreamException;

/**
 *
 * @author Jeroen
 */
public class Reader {

    public static Log log = new Log(Reader.class);
    ByteSearch number = ByteSearch.create("\\d+");
    SimilarityFile outfile;
    SimilarityWritable w = new SimilarityWritable();

    Reader(Datafile outfile) throws IOException {
        this.outfile = new SimilarityFile(outfile);
        outfile.openWrite();
    }

    public void read(Datafile df) throws XMLStreamException, IOException {
        XMLReader r = new XMLReader(df.getInputStream(), "document");
        while (r.hasNext()) {
            HashMap<String, Object> next = r.next();
            if (next != null) {
                //log.info("%s", next);
                w.id = getDocId((String) next.get("reference"));
                HashMapInt<Integer> sources = new HashMapInt();
                for (Object feature : ((ArrayList) next.get("feature"))) {
                    HashMap<String, Object> featureMap = (HashMap) feature;
                    //log.info("%s", featureMap);
                    if (featureMap.get("name").equals("plagiarism")) {
                        Integer sourceId = getDocId((String) featureMap.get("source_reference"));
                        Integer length = Integer.parseInt((String) featureMap.get("this_length"));
                        sources.add(sourceId, length);
                    }
                }
                ArrayMap<Integer, Integer> sorted = new ArrayMap();
                CollectionTools.invert(sources, sorted);
                for (Map.Entry<Integer, Integer> entry : sorted.descending()) {
                    w.source = entry.getValue();
                    w.indexSimilarity = entry.getKey();
                    w.measureSimilarity = entry.getKey();
                    w.write(outfile);
                }
            }
        }
    }

    public void close() {
        outfile.closeWrite();
    }

    public Integer getDocId(String filename) {
        return Integer.parseInt(number.extract(filename));
    }

    public static void main(String[] args) throws XMLStreamException, IOException {
        ArgsParser ap = new ArgsParser(args, "input output");
        Reader r = new Reader(new Datafile(ap.get("output")));
        FSPath path = new FSPath(ap.get("input"));
        for (Datafile df : path.getFiles()) {
            r.read(df);
        }
        r.close();

    }
}
