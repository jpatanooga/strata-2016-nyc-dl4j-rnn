package org.deeplearning4j.examples.rnn.beer.schema.json;

import org.datavec.api.conf.Configuration;
import org.datavec.api.records.listener.RecordListener;
import org.datavec.api.records.reader.SequenceRecordReader;
import org.datavec.api.split.InputSplit;
import org.datavec.api.writable.Writable;
import org.datavec.api.writable.FloatWritable;
import org.datavec.api.writable.IntWritable;

import java.io.DataInputStream;
import java.io.IOException;
import java.net.URI;
import java.util.*;

/**
 * Created by davekale on 9/20/16.
 */
public class BeerReviewSequenceRecordReader extends BeerReviewReader implements SequenceRecordReader {

    private GroupedBeerDictionary beerDictionary;
    private char[] validCharacters;
    private HashMap<Character, Integer> charToIdxMap;
    private int maxExamplesToRead = 0;

    public final char STOPWORD = '~';

    public BeerReviewSequenceRecordReader(String jsonFilePath, String beerDictionaryFilePath, int maxExamplesToRead, char[] validCharacters) throws IOException {
        super(jsonFilePath);
        init();
        charToIdxMap = new HashMap<>();
        this.validCharacters = validCharacters;
        for( int i=0; i<validCharacters.length; i++ ) charToIdxMap.put(validCharacters[i], i);
        assert(!charToIdxMap.containsKey(STOPWORD));
        charToIdxMap.put(STOPWORD, charToIdxMap.size());
        this.beerDictionary = new GroupedBeerDictionary();
        this.beerDictionary.loadBeerEntries(beerDictionaryFilePath);
        this.beerDictionary.printBeerStyleStats();
        this.maxExamplesToRead = maxExamplesToRead;

        // TODO: init reader
        System.out.println("\nFound reviews: " + countReviews() + "\n\n");
    }

    public BeerReviewSequenceRecordReader(String jsonFilePath, String beerDictionaryFilePath, int maxExamplesToRead) throws IOException {
        this(jsonFilePath, beerDictionaryFilePath, maxExamplesToRead, getDefaultCharacterSet());
    }

    public BeerReviewSequenceRecordReader(String jsonFilePath, String beerDictionaryFilePath, char[] validCharacters) throws IOException {
        this(jsonFilePath, beerDictionaryFilePath, 0, validCharacters);
    }

    public BeerReviewSequenceRecordReader(String jsonFilePath, String beerDictionaryFilePath) throws IOException {
        this(jsonFilePath, beerDictionaryFilePath, 0, getDefaultCharacterSet());
    }

    public float rescaleRating(float rating) {
        return (rating - 1) / 2.0f - 1;
    }

    public int getVocabSize() {
        return charToIdxMap.size();
    }

    public int getBeerStyleCount() { return beerDictionary.getBeerStyleCount(); }

    public char convertIndexToCharacter( int idx ) {
        if (idx >= validCharacters.length && idx == charToIdxMap.get(STOPWORD))
            return STOPWORD;
        return validCharacters[idx];
    }

    public int convertCharacterToIndex( char c ){
        return charToIdxMap.get(c);
    }

    @Override
    public List<List<Writable>> sequenceRecord() {
        BeerReview br = null;
        try {
            br = getNextReview();
        } catch (IOException e) {
            return new ArrayList<>();
        }
        ArrayList<Writable> context = new ArrayList<Writable>();
        int styleIndex = beerDictionary.lookupBeerStyleIndexByBeerID(br.beer_id);
        for (int i = 0; i < beerDictionary.getBeerStyleCount(); i++)
            context.add(new FloatWritable(i == styleIndex? 1.0f : 0.0f));
        context.add(new FloatWritable(rescaleRating(br.rating_appearance)));
        context.add(new FloatWritable(rescaleRating(br.rating_aroma)));
        context.add(new FloatWritable(rescaleRating(br.rating_palate)));
        context.add(new FloatWritable(rescaleRating(br.rating_taste)));
        context.add(new FloatWritable(rescaleRating(br.rating_overall)));

        ArrayList<List<Writable>> sequence = new ArrayList<List<Writable>>();
        char[] review = convertReviewToCharacters(br);
        int inputIndex = charToIdxMap.get(STOPWORD);
        for (int j = 0; j < review.length; j++) {
            char outputC = review[j];
            ArrayList<Writable> step = new ArrayList<Writable>();
            if (!charToIdxMap.containsKey(outputC))
                continue;
            int outputIndex = charToIdxMap.get(outputC);
            step.add(new IntWritable(outputIndex));
            for (int i = 0; i < getVocabSize(); i++)
                step.add(new FloatWritable(i == inputIndex ? 1.0f : 0.0f));
            step.addAll(context);
            sequence.add(step);
            inputIndex = outputIndex;
        }
        return sequence;
    }

    @Override
    public boolean hasNext() {
        return ((maxExamplesToRead == 0 || getCount() < maxExamplesToRead) && super.hasNext());
    }

    @Override
    public List<List<Writable>> sequenceRecord(URI uri, DataInputStream dataInputStream) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void initialize(InputSplit inputSplit) throws IOException, InterruptedException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void initialize(Configuration configuration, InputSplit inputSplit) throws IOException, InterruptedException {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<Writable> next() {
        throw new UnsupportedOperationException();
    }

//    @Override
//    public boolean hasNext() {
//        throw new UnsupportedOperationException();
//    }

    @Override
    public List<String> getLabels() {
        return null;
    }
//
//    @Override
//    public void reset() {
//
//    }

    @Override
    public List<Writable> record(URI uri, DataInputStream dataInputStream) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<RecordListener> getListeners() {
        return null;
    }

    @Override
    public void setListeners(RecordListener... recordListeners) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setListeners(Collection<RecordListener> collection) {
        throw new UnsupportedOperationException();
    }
//
//    @Override
//    public void close() throws IOException {
//    }

    @Override
    public void setConf(Configuration configuration) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Configuration getConf() {
        throw new UnsupportedOperationException();
    }

    /** A minimal character set, with a-z, A-Z, 0-9 and common punctuation etc */
    public static char[] getMinimalCharacterSet(){
        List<Character> validChars = new LinkedList<>();
        for(char c='a'; c<='z'; c++) validChars.add(c);
        for(char c='A'; c<='Z'; c++) validChars.add(c);
        for(char c='0'; c<='9'; c++) validChars.add(c);
        char[] temp = {'!', '&', '(', ')', '?', '-', '\'', '"', ',', '.', ':', ';', ' ', '\n', '\t'};
        for( char c : temp ) validChars.add(c);
        char[] out = new char[validChars.size()];
        int i=0;
        for( Character c : validChars ) out[i++] = c;
        return out;
    }

    /** As per getMinimalCharacterSet(), but with a few extra characters */
    public static char[] getDefaultCharacterSet(){
        List<Character> validChars = new LinkedList<>();
        for(char c : getMinimalCharacterSet() ) validChars.add(c);
        char[] additionalChars = {'@', '#', '$', '%', '^', '*', '{', '}', '[', ']', '/', '+', '_',
                '\\', '|', '<', '>'};
        for( char c : additionalChars ) validChars.add(c);
        char[] out = new char[validChars.size()];
        int i=0;
        for( Character c : validChars ) out[i++] = c;
        return out;
    }


    private char[] convertReviewToCharacters( BeerReview br ) {
        // for each line, convert into array
        int currIdx = 0;
        char[] thisLine = br.text.toCharArray(); //s.toCharArray();

        char[] characters = new char[ thisLine.length + 1 ];

        for( int i=0; i< thisLine.length; i++ ){
            if( !charToIdxMap.containsKey(thisLine[i]) ) {
                continue;
            }
            characters[currIdx++] = thisLine[i];
        }
        characters[currIdx] = STOPWORD;
        return characters;
    }
}

