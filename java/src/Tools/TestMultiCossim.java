package Tools;

import TestGeneric.Document;
import io.github.htools.hadoop.Conf;
import io.github.htools.io.Datafile;
import io.github.htools.io.FSPath;
import io.github.htools.io.HPath;
import io.github.htools.io.compressed.ArchiveEntry;
import io.github.htools.io.compressed.ArchiveFile;
import io.github.htools.lib.ArgsParser;
import io.github.htools.lib.Log;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * gives the cosine similarity between two documents
 * @author Jeroen
 */
public class TestMultiCossim {
    public static Log log = new Log(TestMultiCossim.class);

    public static List<Document> loadSourceDocuments(HPath sourcePath) throws IOException {
        List<Document> retVal = new ArrayList<>();
        ArrayList<Datafile> files = sourcePath.getFiles();
        while (files.size() > 0) {
            Datafile file = files.remove(0);
            ArchiveFile sourceFile = ArchiveFile.getReader(file);
            for (ArchiveEntry entry : (Iterable<ArchiveEntry>) sourceFile) {
                Document document = Document.readContent(entry.getName(), entry.readAll());
                retVal.add(document);
            }
        }
        return retVal;
    }
    
    public static void main(String[] args) throws IOException {        
        ArgsParser ap = new ArgsParser(args, "sourcepath number_of_cos");
        
        FSPath source = new FSPath(ap.get("sourcepath"));
        List<Document> docs = loadSourceDocuments(source);
        int nDC = Integer.valueOf(ap.get("number_of_cos"));
        
        if (docs.isEmpty()) {
            return;
        }        
        Random rnd = new Random();
        
        // first calculate a few sims to do the hot spot compliation and then wait a few seconds
        for (int i = 0; i < 100; i++) {
            docs.get(rnd.nextInt(docs.size())).getModel().cossim( docs.get(rnd.nextInt(docs.size())).getModel());            
        }
        try {
            Thread.sleep(2000);            
        } catch (InterruptedException ex) {
            Logger.getLogger(TestMultiCossim.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        // calculate specified number of cos distances between random pairs of documents
        long startTime = System.currentTimeMillis();
        int size = docs.size();
        for (int i = 0; i < nDC; i++) {
            double distance = docs.get(rnd.nextInt(size)).getModel().cossim( docs.get(rnd.nextInt(size)).getModel());            
            System.out.println("distance: " + distance);
            //System.out.println("docA.getModel().magnitude(), docB.getModel().magnitude(), docA.getModel().cossim(docB.getModel()));
        }
        long runningTime = System.currentTimeMillis() - startTime;
        System.out.println("\nNumber of COS evaluations: " + nDC);
        System.out.println("Overall time [s]: " + (float) runningTime / 1000f);
        System.out.println("Avg COS evaluation time [ms]: " + (float) runningTime / nDC);
            
//        log.info("%f %f %f", docA.getModel().magnitude(), docB.getModel().magnitude(), docA.getModel().cossim(docB.getModel()));
        
        // results on pan11/source/source0.tar.lz4
        // Number of COS evaluations: 1000
        // Overall time[s]: 0.407 
        // Avg COS evaluation time[ms]: 0.407
    }
}
