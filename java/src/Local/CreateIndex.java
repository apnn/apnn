package Local;

import TestGeneric.Document;
import io.github.htools.io.Datafile;
import io.github.htools.io.FSPath;
import io.github.htools.io.HPath;
import io.github.htools.io.compressed.ArchiveEntry;
import io.github.htools.io.compressed.ArchiveFile;
import io.github.htools.lib.ArgsParser;
import io.github.htools.lib.Log;
import io.github.htools.search.ByteSearch;
import io.github.htools.type.TermVectorDouble;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;

/**
 * Wrapper to go over the source documents to construct an index
 * run with:
 * java -cp apnn.jar Local.CreateIndex <tdfidfcollectionfolder>
 */
public class CreateIndex {

    public static Log log = new Log(CreateIndex.class);

    public CreateIndex(HPath sourcePath) throws IOException {
        // whatever you ned to prepare your index for loading documents
        
        
        loadSourceDocuments(sourcePath);
        
        // write your index
    }

    /**
     * Load the entire source.tfidf.tar.lz4 collection. For each source document
     * read a Document object is created, and addToDocumentIndex(Document) is
     * called to add it to the index.
     * @param sourcePath
     * @throws IOException 
     */
    public void loadSourceDocuments(HPath sourcePath) throws IOException {
        // get all files in the given folder
        ArrayList<Datafile> files = sourcePath.getFiles();
        while (files.size() > 0) {
            // get first archive file in the collection
            Datafile file = files.remove(0);
            ArchiveFile sourceFile = ArchiveFile.getReader(file);
            for (ArchiveEntry entry : (Iterable<ArchiveEntry>) sourceFile) {
                String docid = getDocID(entry.getName());
                // read the next Document with tfidf vector from the archivefile
                Document document = Document.readTFIDF(docid, entry.readAll());
                addDocumentToIndex(document);
            }
        }
    }
    
    // implement this to add the next Document to the index
    public void addDocumentToIndex(Document doc) {
        // doc.docid is the collection id for the source document
        String docid = doc.docid;
        // doc.getModel can be casted to TermVectorDouble to process
        // and then go over each term (String containing the
        // termID (number)) and tfidf
        TermVectorDouble model = (TermVectorDouble)doc.getModel();
        for (Map.Entry<String, Double> entry : model.entrySet()) {
           int termid = Integer.parseInt(entry.getKey());
           double tfidf = entry.getValue();
           log.info("%5d %6d %10.6f", docid, termid, tfidf);
        }
        // if needed you can do model.cossim(othermodel) to get the cosine
        // between documents.
    }
    
    ByteSearch number = ByteSearch.create("\\d+");
    public String getDocID(String filename) {
        return number.extract(filename);
    }
    
    
    public static void main(String[] args) throws IOException, ClassNotFoundException {
        ArgsParser ap = new ArgsParser(args, "sourcepath");
        FSPath source = new FSPath(ap.get("sourcepath"));
        new CreateIndex(source);
    }
}
