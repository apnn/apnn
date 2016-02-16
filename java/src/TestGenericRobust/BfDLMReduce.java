package TestGenericRobust;

import io.github.htools.lib.Log;

/**
 * FDM is scored here as query likelihood like in the original, however Zhai's
 * Dirichlet smoothed LM notation is rank equivalent and faster.
 *
 * @author jeroen
 */
public class BfDLMReduce extends BfFDMReduce {

    public static final Log log = new Log(BfDLMReduce.class);
    
    @Override
    public double score(FDMQuery query, FDMDoc doc, FDMVoc voc) {
        return doc.getDLM(query, voc, params);
    }
}
