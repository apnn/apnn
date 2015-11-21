package TrecGT;

import SimilarityFile.SimilarityFile;
import SimilarityFile.SimilarityWritable;
import io.github.htools.io.Datafile;
import io.github.htools.io.FSPath;
import io.github.htools.lib.ArgsParser;
import io.github.htools.lib.Log;
import io.github.htools.search.ByteSearch;
import java.io.IOException;
/**
 *
 * @author Jeroen
 */
public class Reader {

    public static Log log = new Log(Reader.class);
    ByteSearch number = ByteSearch.create("\\d+");
    SimilarityFile outfile;
    SimilarityWritable w = new SimilarityWritable();

    Reader(Datafile outfile) {
        this.outfile = new SimilarityFile(outfile);
        outfile.openWrite();
    }

    public void read(Datafile df) {
        for (String line : df.readLines()) {
            String[] part = line.split("\\s+");
            if (part[3].equals("1")) {
                w.id = part[0];
                w.source = part[2];
                w.indexSimilarity = 1;
                w.measureSimilarity = 1;
                w.write(outfile);
            }
        }
    }

    public void close() {
        outfile.closeWrite();
    }

    public static void main(String[] args) {
        ArgsParser ap = new ArgsParser(args, "input output");
        Reader r = new Reader(new Datafile(ap.get("output")));
        FSPath path = new FSPath(ap.get("input"));
        for (Datafile df : path.getFiles()) {
            r.read(df);
        }
        r.close();
    }
}
