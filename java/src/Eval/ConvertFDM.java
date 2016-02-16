package Eval;

import io.github.htools.hadoop.Conf;
import io.github.htools.io.Datafile;
import io.github.htools.io.FSPath;
import java.io.IOException;

/**
 * A generic class to compute an evaluation metric over a set of retrieved
 * nearest neighbor source documents for the collection of suspicious documents.
 * For each suspicious document the evaluation metric is based on the comparison
 * between the retrieved set of k-most nearest neighbors and a ground truth set
 * of k-most nearest neighbors. Both the retrieved set and ground truth should
 * be input as a SimilarityFile. When constructing a single ground truth file is
 * given, along with k which is the maximum rank considered (top-k).
 *
 * Important! The SimilarityFiles should be in order of descending similarity
 * score per suspicious document.
 *
 * @author Jeroen
 */
public abstract class ConvertFDM {

    public static void main(String[] args) throws IOException, Exception {
        Conf conf = new Conf(args, "input output -r rank");

        int maxrank = conf.getInt("rank", 1000);
        FSPath inPath = conf.getFSPath("input");
        Datafile out = conf.getFSFile("output");
        out.openWrite();
        for (Datafile df : inPath.getFiles()) {
            int rank = 1;
            for (String f : df.readLines()) {
                String part[] = f.split("\\s+");
                out.printf("%s Q0 %s %d %s %s\n", df.getName(), part[1], rank++, part[2], "run");
                if (rank > maxrank)
                    break;
             }
            df.closeRead();
        }
        out.closeWrite();
    }
}
