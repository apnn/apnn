package TestGenericRobust;

import io.github.htools.lib.Log;

/**
 * FDM is scored here as query likelihood like in the original, however Zhai's
 * Dirichlet smoothed LM notation is rank equivalent and faster.
 *
 * @author jeroen
 */
public class BfLMReduce extends BfFDMReduce {

    public static final Log log = new Log(BfLMReduce.class);
    
    public double score(FDMQuery query, FDMDoc doc, FDMVoc voc) {
        return doc.getLM(query, voc, params);
    }
}
