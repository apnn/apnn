package TestGenericRobust;

import TestGeneric.Document;
import io.github.htools.collection.ArrayMap;
import io.github.htools.collection.HashMapInt;
import io.github.htools.hadoop.io.buffered.Writable;
import io.github.htools.io.buffer.BufferDelayedWriter;
import io.github.htools.io.buffer.BufferReaderWriter;
import io.github.htools.lib.Log;
import io.github.htools.lib.Profiler;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;

/**
 * @author Jeroen
 */
public class FDMDoc extends Writable {
    
    public static Log log = new Log(FDMDoc.class);
    FDMQuery q;
    String id;
    int doctf;
    HashMapInt<Long> independent;
    HashMapInt<Long> ordered;
    HashMapInt<Long> unordered;
    double score = Double.MIN_VALUE;
    int mu = 2500;
    
    public FDMDoc() {
    }
    
    public FDMDoc(Document document, FDMQuery q) throws IOException {
        this.q = q;
        this.id = document.getId();
        this.doctf = document.getTerms().size();
        independent = new HashMapInt(getIndependent(document, q));
        //log.info("indepedent %d", independent.size());
        if (independent.size() > 0) {
            ordered = new HashMapInt(getOrdered(document, q));
            //log.info("ordered %d", ordered.size());
            unordered = new HashMapInt(getUnordered(document, q));
            //log.info("unordered %d", unordered.size());
        }
    }
    
    public FDMDoc clone() {
        FDMDoc clone = new FDMDoc();
        clone.id = id;
        clone.doctf = doctf;
        clone.independent = independent;
        clone.ordered = ordered;
        clone.unordered = unordered;
        return clone;
    }
    
    public double getFDM(FDMQuery query, FDMVoc voc, FDMParameters parameters) {
        if (score == Double.MIN_VALUE) {
            Profiler.startTime("getlm");
            score = 0;
            double alpha = mu / (double) (mu + doctf);
            //log.info("getLM %s %f %f %f", this.id, parameters.lambdai, parameters.lambdao, parameters.lambdau);
            //log.info("getLM %s %s %s %s", this.id, independent, ordered, unordered);
            score += parameters.lambdai * scoreGroup(independent, voc.termFrequency, voc.getCollectionsize(), alpha);
            score += parameters.lambdao * scoreGroup(ordered, voc.orderedFrequency, voc.getCollectionsize(), alpha);
            score += parameters.lambdau * scoreGroup(unordered, voc.unorderedFrequency, voc.getCollectionsize(), alpha);
            Profiler.addTime("getlm");
        }
        return score;
    }
    
    public double getLM(FDMQuery query, FDMVoc voc, FDMParameters parameters) {
        if (score == Double.MIN_VALUE) {
            Profiler.startTime("getlm");
            score = 0;
            double alpha = mu / (double) (mu + doctf);
            //log.info("getLM %s %f %f %f", this.id, parameters.lambdai, parameters.lambdao, parameters.lambdau);
            //log.info("getLM %s %s %s %s", this.id, independent, ordered, unordered);
            score += parameters.lambdai * scoreGroup(independent, voc.termFrequency, voc.getCollectionsize(), alpha) / query.termlist.size();
            Profiler.addTime("getlm");
        }
        return score;
    }
    
    public double getDLM(FDMQuery query, FDMVoc voc, FDMParameters parameters) {
        if (score == Double.MIN_VALUE) {
            Profiler.startTime("getlm");
            score = 0;
            double alpha = mu / (double) (mu + doctf);
            //log.info("getLM %s %f %f %f", this.id, parameters.lambdai, parameters.lambdao, parameters.lambdau);
            //log.info("getLM %s %s %s %s", this.id, independent, ordered, unordered);
            for (Map.Entry<Long, Integer> entry : independent.entrySet()) {
                Integer cf = voc.termFrequency.get(entry.getKey());
                double ptc = cf / voc.getCollectionsize();
                score += Math.log(1 + entry.getValue() / (mu * ptc));
            }
            score += query.termlist.size() * Math.log(alpha);
            Profiler.addTime("getlm");
        }
        return score;
    }
    
    public double scoreGroup(HashMapInt<Long> tf, HashMapInt<Long> cf, double collectionsize, double alpha) {
        double score = 0;
        for (Map.Entry<Long, Integer> entry : tf.entrySet()) {
            double ptc = cf.get(entry.getKey()) / collectionsize;
            score += Math.log((1 - alpha) * (entry.getValue() / (double) doctf) + alpha * ptc);
            //log.info("score1 ptc %f alpha %f tf %d score %e", ptc, alpha, entry.getValue(), score);
        }
        for (Map.Entry<Long, Integer> entry : cf.entrySet()) {
            if (!tf.containsKey(entry.getKey())) {
                double ptc = entry.getValue() / collectionsize;
                score += Math.log(alpha * ptc);
                //log.info("score2 ptc %f alpha %f score %e", ptc, alpha, score);
            }
        }
        return score;
    }
    
    public static ArrayList<Long> getIndependent(Document doc, FDMQuery query) {
        ArrayList<Long> result = new ArrayList();
        long match = 0;
        for (int i = 0; i < doc.getTerms().size(); i++) {
            String term = doc.getTerms().get(i);
            if (query.termset.contains(term)) {
                int querypos = query.uniqtermlist.indexOf(term);
                result.add(1l << querypos);
            }
        }
        return result;
    }
    
    public static ArrayList<Long> getOrdered(Document doc, FDMQuery query) {
        ArrayList<Long> result = new ArrayList();
        long match = 0;
        for (int i = 0; i < doc.getTerms().size(); i++) {
            String term = doc.getTerms().get(i);
            if (query.termset.contains(term)) {
                for (int querypos = query.termlist.indexOf(term);
                        querypos > -1; querypos = indexOf(query.termlist, term, querypos + 1)) {
                    match = 1l << querypos;
                    for (int j = querypos + 1, termpos = i + 1;
                            j < query.termlist.size() && termpos < doc.getTerms().size()
                            && doc.getTerms().get(termpos).equals(query.termlist.get(j));
                            j++, termpos++) {
                        match += 1l << j;
                        result.add(match);
                    }
                }
            }
        }
        return result;
    }
    
    public static ArrayList<Long> getUnordered(Document doc, FDMQuery query) {
        ArrayList<Long> result = new ArrayList();
        ArrayMap<Integer, Long> pos = new ArrayMap();
        for (int i = 0; i < doc.getTerms().size(); i++) {
            String term = doc.getTerms().get(i);
            if (query.termset.contains(term)) {
                int querypos = query.uniqtermlist.indexOf(term);
                pos.add(i, 1l << querypos);
            }
        }
        for (int j = 1; j < pos.size(); j++) {
            int endpos = pos.getKey(j);
            for (int i = 0; i < j; i++) {
                if (endpos - pos.getKey(i) < (j - i + 1) * 4) {
                    long match = 0;
                    for (int k = i + 1; k < j; k++) {
                        match |= pos.getValue(k);
                    }
                    if ((match & pos.getValue(j)) == 0) {
                        match |= pos.getValue(j);
                        if ((match & pos.getValue(i)) == 0) {
                            match |= pos.getValue(i);
                            int bits = Long.bitCount(match);
                            if (endpos - pos.getKey(i) < bits * 4) {
                                result.add(match);
                            }
                        }
                    }
                }
            }
        }
        return result;
    }
    
    public static int indexOf(ArrayList<String> list, String term, int offset) {
        for (int i = offset; i < list.size(); i++) {
            if (list.get(i).equals(term)) {
                return i;
            }
        }
        return -1;
    }
    
    @Override
    public void write(BufferDelayedWriter writer) {
        writer.write(id);
        writer.write(doctf);
        write(writer, this.independent);
        write(writer, this.ordered);
        write(writer, this.unordered);
    }
    
    public void write(BufferDelayedWriter writer, HashMapInt<Long> map) {
        writer.write(map.sumValue());
        for (Map.Entry<Long, Integer> entry : map.entrySet()) {
            for (int i = 0; i < entry.getValue(); i++) {
                writer.write(entry.getKey());
            }
        }
    }
    
    @Override
    public void readFields(BufferReaderWriter reader) {
        id = reader.readString();
        doctf = reader.readInt();
        independent = read(reader);
        ordered = read(reader);
        unordered = read(reader);
    }
    
    public HashMapInt<Long> read(BufferReaderWriter reader) {
        HashMapInt<Long> result = new HashMapInt();
        int size = reader.readInt();
        for (int i = 0; i < size; i++) {
            result.add(reader.readLong());
        }
        return result;
    }
    
}
