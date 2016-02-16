package PanDetection;

import TestGeneric.Document;
import edu.emory.mathcs.backport.java.util.Arrays;
import io.github.htools.collection.ArrayMap;
import io.github.htools.collection.HashMapInt;
import io.github.htools.collection.HashMapList;
import io.github.htools.collection.SelectionInterval;
import io.github.htools.fcollection.FHashMapIntList;
import io.github.htools.hadoop.io.buffered.Writable;
import io.github.htools.io.buffer.BufferDelayedWriter;
import io.github.htools.io.buffer.BufferReaderWriter;
import io.github.htools.lib.ByteTools;
import io.github.htools.lib.Log;
import io.github.htools.lib.Profiler;
import io.github.htools.search.ByteSearchPosition;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

/**
 * Indexes files from a collection of source documents in the PAN11 collection,
 * using a simplified implementation
 * of the algorithm proposed in Kasprzak and Brandejs: "Improving the reliability of
 * the plagiarism detection system." Lab Report for PAN at CLEF, pages 359–366, 2010.
 * <p/>
 * The index stores hashcodes for every chunk of 5 words, and for a given suspicous
 * documents retrieves passages that are matched in source documents of at least 20 of
 * those 5-word chunks in common, while the
 * maximum allowed gap between 2 chunks is no more than 100 words (the original paper
 * used 50 words).
 *
 * @author Jeroen
 */
public class AnnChunk {

    public static Log log = new Log(AnnChunk.class);
    MessageDigest md;
    protected FHashMapIntList<Doc> mapChunks;
    int termdensitity = 100;
    int minchunks = 20;

    public AnnChunk() {
        try {
            //log.info("AnnChunk");
            md = MessageDigest.getInstance("MD5");
            mapChunks = new FHashMapIntList(1000000);
        } catch (NoSuchAlgorithmException ex) {
            log.fatalexception(ex, "AnnChunk");
        }
    }

    protected void addDocument(Document document) {
        Profiler.startTime("addDocument");
        Doc doc = new Doc(document, getFingerprint(document));
        for (int chunk : doc.chunks) {
            ObjectArrayList<Doc> list = mapChunks.getList(chunk);
            if (list.size() == 0 || list.get(list.size() - 1) != doc) {
                list.add(doc);
            }
        }
        Profiler.addAvgTime("addDocument");
    }

    protected ArrayList<Passage> getDocuments(Document query) {
        //log.info("getDocuments %s", query.docid);
        Profiler.startTime("getPassages");
        Doc doc = new Doc(query, getFingerprint(query));
        HashMapInt<Doc> docCount = new HashMapInt();
        for (int chunk : doc.chunks) {
            ObjectArrayList<Doc> list = mapChunks.get(chunk);
            if (list != null) {
                docCount.addAll(list);
            }
        }
        ArrayList<Passage> passages = new ArrayList();
        for (Map.Entry<Doc, Integer> entry : docCount.entrySet()) {
            if (entry.getValue() >= minchunks) {
                passages.addAll(getPlagiarism(doc, entry.getKey()));
            }
        }
        Profiler.addAvgTime("getPassages");
        return passages;
    }

    private ArrayList<Passage> getPlagiarism(Doc query, Doc source) {
        ArrayList<Passage> candidatePassages = new ArrayList();
        ArrayList<Passage> sourcePassages = getPassages(source, query);
        for (Passage sourcePassage : sourcePassages) {
            HashSet<Integer> sourceHashSet;
            sourceHashSet = new HashSet<Integer>(sourcePassage.hash.keySet());
            candidatePassages.addAll(getPassages(query, sourceHashSet));
        }

        Collections.sort(candidatePassages, passageComparator);
        ArrayList<Passage> result = new ArrayList();
        SelectionInterval selected = new SelectionInterval();
        for (Passage candidate : candidatePassages) {
            boolean overlaps = selected.overlaps(candidate.queryoffset, candidate.queryoffset + candidate.querylength);
            if (!overlaps) {
                result.add(candidate);
                selected.addInterval(candidate.queryoffset, candidate.queryoffset + candidate.querylength);
            }
        }
        resolveSourcePassages(result, source);
        return result;
    }

    PassageComparator passageComparator = new PassageComparator();

    private static class PassageComparator implements Comparator<Passage> {

        @Override
        public int compare(Passage o1, Passage o2) {
            int comp = o2.querylength - o1.querylength;
            if (comp == 0) {
                comp = o1.queryoffset - o2.queryoffset;
            }
            return comp;
        }
    }

    private void mergePassages(ArrayList<Passage> passages) {
        for (int i = passages.size() - 2; i >= 0; i--) {
            Passage second = passages.get(i + 1);
            Passage first = passages.get(i);
            int gap = second.queryoffset - (first.queryoffset + first.querylength);
            if (gap < 4000) {
                Passage merged = first.merge(second);
                if (termdensitity * merged.avgChunk() >= 1) {
                    first = merged;
                    passages.set(i, first);
                    passages.remove(i + 1);
                    while (i < passages.size() - 1) {
                        second = passages.get(i + 1);
                        gap = second.queryoffset - (first.queryoffset + first.querylength);
                        if (gap < 4000) {
                            merged = first.merge(second);
                            if (termdensitity * merged.avgChunk() >= 1) {
                                first = merged;
                                passages.set(i, first);
                                passages.remove(i + 1);
                            } else {
                                break;
                            }
                        } else {
                            break;
                        }
                    }
                }
            }
        }
        for (int i = passages.size() - 1; i >= 0; i--) {
            Passage first = passages.get(i);
            if (first.hash.size() < minchunks || termdensitity * first.avgChunk() < 1) {
                passages.remove(i);
            }
        }
    }

    private ArrayList<Passage> getPassages(Doc query, Doc source) {
        HashSet<Integer> bset = new HashSet(source.chunks.length);
        for (int bb : source.chunks) {
            bset.add(bb);
        }
        return getPassages(query, bset);
    }

    private ArrayList<Passage> getPassages(Doc query, HashSet<Integer> bset) {
        ArrayList<Passage> result = new ArrayList();
        int count = 0;
        int lastpos = -1000;
        int firstpos = -1000;
        ArrayMap<Integer, Integer> passagehash = null;
        for (int apos = 0; apos < query.chunks.length; apos++) {
            int hash = query.chunks[apos];
            if (bset.contains(hash)) {
                if (apos - lastpos >= termdensitity) {
                    if (count > 1) {
                        Passage passage = new Passage();
                        passage.queryid = query.getId();
                        passage.startpos = firstpos;
                        passage.endpos = lastpos;
                        passage.queryoffset = query.getTokenPostions().get(firstpos).start;
                        int lpos = query.getTokenPostions().get(lastpos + 4).end;
                        passage.querylength = lpos - passage.queryoffset;
                        passage.hash = passagehash;
                        result.add(passage);
                     } else if (count > 1) {
                    }
                    passagehash = new ArrayMap();
                    passagehash.add(hash, apos);
                    firstpos = apos;
                    lastpos = apos;
                    count = 1;
                } else {
                    passagehash.add(hash, apos);
                    count++;
                    lastpos = apos;
                }
            }
        }
        if (count > 1) {
            Passage passage = new Passage();
            passage.queryid = query.getId();
            passage.startpos = firstpos;
            passage.endpos = lastpos;
            passage.queryoffset = query.getTokenPostions().get(firstpos).start;
            int lpos = query.getTokenPostions().get(lastpos + 4).end;
            passage.querylength = lpos - passage.queryoffset;
            passage.hash = passagehash;
            result.add(passage);
        }
        mergePassages(result);
        return result;
    }

    private void resolveSourcePassages(ArrayList<Passage> passages, Doc source) {
        HashMapList<Integer, Integer> hashPositions = new HashMapList();
        for (int pos = 0; pos < source.chunks.length; pos++) {
            hashPositions.add(source.chunks[pos], pos);
        }
        for (Passage p : passages) {
            int firstpos = Integer.MAX_VALUE;
            int lastpos = -1;
            HashSet<Integer> hashset = new HashSet<Integer>(p.hash.keySet());
            for (int hash : hashset) {
                ArrayList<Integer> positions = hashPositions.get(hash);
                firstpos = Math.min(firstpos, positions.get(positions.size() - 1));
                lastpos = Math.max(lastpos, positions.get(0));
            }
            p.sourceoffset = source.getTokenPostions().get(firstpos).start;
            int sourceend = source.getTokenPostions().get(lastpos + 4).end;
            p.sourcelength = sourceend - p.sourceoffset;
            p.sourceid = source.getId();
        }
    }

    /**
     * @param document
     * @return a list ofg 5-chunk hashcodes
     */
    private int[] getFingerprint(Document document) {
        String[] chunk = new String[5];
        ArrayList<String> terms = document.getTermsStopwords();
        int[] hash = new int[terms.size() - 4];
        for (int i = 4; i < terms.size(); i++) {
            for (int c = 0; c < 5; c++) {
                chunk[c] = terms.get(i - c);
            }
            Arrays.sort(chunk);
            md.reset();
            for (String term : chunk) {
                md.update(ByteTools.toBytes(term));
            }
            byte[] digest = md.digest();
            int ch1 = digest[0] & 0xFF;
            int ch2 = digest[1] & 0xFF;
            int ch3 = digest[2] & 0xFF;
            int ch4 = digest[3] & 0xFF;
            int hashCode = ((ch1 << 24) + (ch2 << 16) + (ch3 << 8) + (ch4));
            hash[i - 4] = hashCode;
        }
        return hash;
    }

    private class Doc extends Document {

        int[] chunks;
        ArrayList<ByteSearchPosition> tokenpositions;

        public Doc(Document document, int[] chunks) {
            super(document);
            this.chunks = chunks;
        }

        public ArrayList<ByteSearchPosition> getTokenPostions() {
            if (tokenpositions == null) {
                tokenpositions = extractor.getTokenPositions(this);
            }
            return tokenpositions;
        }
    }

    static class Passage extends Writable {

        String queryid;
        String sourceid;
        int startpos;
        int endpos;
        int queryoffset;
        int querylength;
        int sourceoffset;
        int sourcelength;
        ArrayMap<Integer, Integer> hash;

        public Passage merge(Passage p) {
            Passage c = new Passage();
            c.startpos = startpos;
            c.endpos = p.endpos;
            c.queryid = queryid;
            c.sourceid = sourceid;
            c.hash = hash.clone();
            c.hash.addAll(p.hash.entrySet());
            c.queryoffset = queryoffset;
            c.querylength = (p.queryoffset + p.querylength) - c.queryoffset;
            return c;
        }

        public double avgChunk() {
            return hash.size() / (double) (endpos - startpos);
        }

        @Override
        public void write(BufferDelayedWriter writer) {
            writer.write(queryid);
            writer.write(sourceid);
            writer.write(queryoffset);
            writer.write(querylength);
            writer.write(sourceoffset);
            writer.write(sourcelength);
        }

        @Override
        public void readFields(BufferReaderWriter reader) {
            queryid = reader.readString();
            sourceid = reader.readString();
            queryoffset = reader.readInt();
            querylength = reader.readInt();
            sourceoffset = reader.readInt();
            sourcelength = reader.readInt();
        }
    }
}
