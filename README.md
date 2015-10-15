## Approximate Nearest Neighbor

- cross validation: Eval.CrossValidate

When a model has parameters to tune, cross validation will be simulated by precomputing full runs using a sweep over the parameters, and then running CrossValidate with the grounttruth file, rank depth (default=10), all parameterResultFiles and number of folds (default=10). Wil return the metric (now NDCG) over all documents using the parameters set for each fold to the maximum of the remaining (n-1) folds.  

---

- document representation: TestGeneric.Documents

These Documents are read automatically, tokenized, lowercased, stop words removed, and then feeded to the index using the addDocument() method. Accessing a document, you will find a getId() and a getModel() method. The id is the collection id for the document to be referenced in the results. The model is basically a HashMap<String, Integer> containing the terms and frequency in the document. On the model's class (TermVectorInt) you can use the cossim() method on another vector to compute the cosine similarity. Your index only has to return the K-most similar documents, therefore if more documents are scanned, you can compute the cosine to limit the returned results to the documents with the highest cosine with the suspicious document.

- test your index: class MyTest extends TestGeneric.TestGeneric

Implement the getIndex() method to return your custom index. 

In the constructor, the setupOutput(resultFile) will setup a result file to write the results to, loadSourceDocuments() will read the files in the sourcePath and for every Document read call addDocument on your index, streamSuspiciousDocuments will then iterate over the query documents and call getDocuments() on your index to retrieve the approximate nearest neighbors and write these to the result file

- implement a custom index: class MyIndex extends TestGeneric.AnnIndex 

Look for an example at MinHash.AnnMinHash. Implements the methods addDocument() to add a document to the index and the method getDocuments() that finds a list of K-nearest neighbors for a given document. 

If you can use some kind of fingerprint method to first create a fingerprint of some generic type T, create that and use that for retrieval you can, otherwise just return null in getFingerprint() and operate straight on the document parameter in getDocuments().

- evaluate: java Eval.NDCG pan11/bruteforce results

where pan11/bruteforce is the bruteforce groundtruth file on beehub. Using only a small part of the collection will result in an extremely low nDCG though, but you get the idea.

- Toy Example: MinHash.TestMinHash 

It takes a long time and over 4G memory to run the entire set, but alternatively you can run it with (assuming the collection is in pan11):
java MinHash.TestMinHash pan11/source.tar.lz4/source1.tar.lz4 pan11/suspicious.tar.lz4/suspicious1.tar.lz4 results




