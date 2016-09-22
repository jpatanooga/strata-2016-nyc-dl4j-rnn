package org.deeplearning4j.examples.rnn.beer;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.util.*;

import org.deeplearning4j.datasets.iterator.DataSetIterator;
import org.deeplearning4j.examples.rnn.beer.schema.json.Beer;
import org.deeplearning4j.examples.rnn.beer.schema.json.BeerDictionary;
import org.deeplearning4j.examples.rnn.beer.schema.json.BeerReview;
import org.deeplearning4j.examples.rnn.beer.schema.json.BeerReviewReader;
import org.deeplearning4j.examples.rnn.beer.schema.json.GroupedBeerDictionary;
import org.nd4j.linalg.api.ndarray.INDArray;
import org.nd4j.linalg.dataset.DataSet;
import org.nd4j.linalg.dataset.api.DataSetPreProcessor;
import org.nd4j.linalg.factory.Nd4j;

public class BeerReviewCharacterIterator implements DataSetIterator {
	private static final long serialVersionUID = -7287833919126626356L;
	private static final int MAX_SCAN_LENGTH = 200; 
	private char[] validCharacters;
	private Map<Character,Integer> charToIdxMap;
	private char[] fileCharacters;
	private int exampleLength = 0;
	private int miniBatchSize;
	private int numExamplesToFetch = 0;
	private int examplesSoFar = 0;
	private Random rng;
	private int numCharacters = 0;
	public static final char STOPWORD = 0;

	// account for beer review rating columns
	int extraRatingColumns = 5;
	
	BeerReviewReader reviewReader = null;
	BeerDictionary beerDictionary = null;
	
	public BeerReviewCharacterIterator(String path, String beerDictionaryFilePath, int miniBatchSize, int exampleLength, int numExamplesToFetch ) throws IOException {
		this(path, beerDictionaryFilePath, Charset.defaultCharset(),miniBatchSize,exampleLength,numExamplesToFetch,getDefaultCharacterSet(), new Random());
	}
	
	/**
	 * @param textFilePath Path to text file to use for generating samples
	 * @param beerDictionaryFilePath beer dictionary
	 * @param textFileEncoding Encoding of the text file. Can try Charset.defaultCharset()
	 * @param miniBatchSize Number of examples per mini-batch
	 * @param exampleLength Number of characters in each input/output vector
	 * @param numExamplesToFetch Total number of examples to fetch (must be multiple of miniBatchSize). Used in hasNext() etc methods
	 * @param validCharacters Character array of valid characters. Characters not present in this array will be removed
	 * @param rng Random number generator, for repeatability if required
	 *  of no new line characters, to avoid scanning entire file)
	 * @throws IOException If text file cannot  be loaded
	 */
	public BeerReviewCharacterIterator(String textFilePath, String beerDictionaryFilePath, Charset textFileEncoding, int miniBatchSize, int exampleLength,
			int numExamplesToFetch, char[] validCharacters, Random rng) throws IOException {
		if( !new File(textFilePath).exists()) throw new IOException("Could not access file (does not exist): " + textFilePath);
		if(numExamplesToFetch % miniBatchSize != 0 ) throw new IllegalArgumentException("numExamplesToFetch must be a multiple of miniBatchSize");
		if( miniBatchSize <= 0 ) throw new IllegalArgumentException("Invalid miniBatchSize (must be >0)");
		this.validCharacters = validCharacters;
		this.exampleLength = exampleLength;
		this.miniBatchSize = miniBatchSize;
		this.numExamplesToFetch = numExamplesToFetch;
		this.rng = rng;
		
		//Store valid characters is a map for later use in vectorization
		charToIdxMap = new HashMap<>();
		for( int i=0; i<validCharacters.length; i++ ) charToIdxMap.put(validCharacters[i], i);
		assert(!charToIdxMap.containsKey(STOPWORD));
		charToIdxMap.put(STOPWORD, validCharacters.length);
		numCharacters = validCharacters.length + 1;
		
		//Load file and convert contents to a char[] 
		boolean newLineValid = charToIdxMap.containsKey('\n');
		
		this.beerDictionary = new GroupedBeerDictionary();
		this.beerDictionary.loadBeerEntries(beerDictionaryFilePath);
		this.beerDictionary.printBeerStyleStats();
		
		this.reviewReader = new BeerReviewReader( textFilePath );
        this.reviewReader.init();

		int totalExamplesinDataset = this.reviewReader.countReviews();
		System.out.println("\nFound reviews: " + totalExamplesinDataset + "\n\n");
		if (this.numExamplesToFetch <= 0)
			this.numExamplesToFetch = totalExamplesinDataset;
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
				'\\', '|', '<', '>', '=', '`'};
		for( char c : additionalChars ) validChars.add(c);
		char[] out = new char[validChars.size()];
		int i=0;
		for( Character c : validChars ) out[i++] = c;
		return out;
	}
	
	public char convertIndexToCharacter( int idx ) {
		if (idx == validCharacters.length)
			return STOPWORD;
		return validCharacters[idx];
	}
	
	public int convertCharacterToIndex( char c ){ return charToIdxMap.get(c); }
	
	public char getRandomCharacter(){
		return validCharacters[(int) (rng.nextDouble()*numCharacters)];
	}

	public boolean hasNext() {
		return (numExamplesToFetch <= 0 || examplesSoFar + miniBatchSize <= numExamplesToFetch) && reviewReader.hasNext();
	}

	public DataSet next() {
		return next(miniBatchSize);
	}

	private char[] convertReviewToCharacters( BeerReview br ) {
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

	public DataSet next(int miniBatchSize) {
		
//		if(numExamplesToFetch > 0 && examplesSoFar + miniBatchSize > numExamplesToFetch ) {
//			throw new NoSuchElementException();
//		}
		
		//System.out.println( "NEXT> batch size: " + miniBatchSize );
		
		//Randomly select a subset of the file. No attempt is made to avoid overlapping subsets
		// of the file in the same minibatch
		int maxLength = 0;
		List<BeerReview> reviews = new ArrayList<>();
		while(reviewReader.hasNext() && reviews.size() < miniBatchSize) {
			BeerReview review = null;
			try {
				//br = this.reviewReader.getNextReview();
				review = this.reviewReader.getNextFilteredReview((GroupedBeerDictionary) this.beerDictionary);
			} catch (IOException e) {
				e.printStackTrace();
				throw new NoSuchElementException();
			}
			if (review == null)
				break;
			maxLength = review.text.length() > maxLength ? review.text.length() : maxLength;
			reviews.add(review);
		}
		if (this.exampleLength > 0)
			maxLength = this.exampleLength;

		int beerStyleColumnCount = this.beerDictionary.getBeerStyleCount();
		int inputColumnCount = numCharacters + extraRatingColumns + beerStyleColumnCount;
		int outputColumnCount = numCharacters;

		//Allocate space: { mini-batch size, number columns, number timesteps }
		INDArray input  = Nd4j.zeros(new int[]{ reviews.size(), inputColumnCount, maxLength });
		INDArray labels = Nd4j.zeros(new int[]{ reviews.size(), outputColumnCount, maxLength });
		INDArray mask   = Nd4j.zeros(new int[]{ reviews.size(), maxLength });

		for (int miniBatchIndex = 0; miniBatchIndex < reviews.size(); miniBatchIndex++) {
			BeerReview br = reviews.get(miniBatchIndex);
			// JOIN: br.beer_id -> beer.style -> beerStyle.index
			// MEANING: we need to pre-cache this join in the beer dictionary
			// SO: for each beer: link beer_id -> beer_style_index

			int styleIndex = this.beerDictionary.lookupBeerStyleIndexByBeerID( br.beer_id );
			String styleFull = this.beerDictionary.lookupBeerStyleByBeerID( br.beer_id );
			
//			System.out.println( "Style Index Lookup: " + br.beer_id + " -> " + styleIndex );
//
//			System.out.println( br.text );
//			System.out.println( br.rating_aroma );
			fileCharacters = this.convertReviewToCharacters( br );
			
			if (fileCharacters.length < 10) {
				System.err.println( "Small review text length! [count: " + this.reviewReader.getCount() + "]" );
				System.err.println( "Text: " + br.text );
			}
			
			int startIdx = 0; //(int) (rng.nextDouble()*maxStartIdx);
			int endIdx = startIdx + maxLength;
			if (endIdx > fileCharacters.length) {
				endIdx = fileCharacters.length;
			}
			fileCharacters[endIdx-1] = STOPWORD;

//			System.out.println( "debug> startIdx: " + startIdx + ", endIdx: " + endIdx );

			int staticColumnBaseOffset = numCharacters;
			int styleColumnBaseOffset = staticColumnBaseOffset + numRatings();
			int styleIndexColumn = styleColumnBaseOffset + styleIndex; // add the base to the index
			
//			System.out.println( "style index base: " + styleColumnBaseOffset + ", offset: " + styleIndexColumn + ", Name: " + styleFull );

			char currChar = STOPWORD; //fileCharacters[ startIdx ]
			int currCharIdx = convertCharacterToIndex(currChar);
			int characterTimeStep = 0;
//			for ( int j = startIdx + 1; j <= endIdx; j++, characterTimeStep++ ){
			for ( int j = startIdx; j < endIdx; j++, characterTimeStep++ ){
				char nextChar = STOPWORD;
				
				try {
				   nextChar = fileCharacters[j];
				} catch (NullPointerException npe) {
					System.err.println("bad review: " + br.text);
				}
				int nextCharIdx = convertCharacterToIndex(nextChar);

				// mini-batch-index, column-index, timestep-index -> 1.0
				// for this mini batch example
				//		input -> set the column index for the character id-index -> at the current timestep (c)
				input.putScalar(new int[]{ miniBatchIndex, currCharIdx, characterTimeStep }, 1.0);
				input.putScalar(new int[]{ miniBatchIndex, staticColumnBaseOffset, characterTimeStep }, br.rating_appearance );
				input.putScalar(new int[]{ miniBatchIndex, staticColumnBaseOffset + 1, characterTimeStep }, br.rating_aroma );
				input.putScalar(new int[]{ miniBatchIndex, staticColumnBaseOffset + 2, characterTimeStep }, br.rating_overall );
				input.putScalar(new int[]{ miniBatchIndex, staticColumnBaseOffset + 3, characterTimeStep }, br.rating_palate );
				input.putScalar(new int[]{ miniBatchIndex, staticColumnBaseOffset + 4, characterTimeStep }, br.rating_taste );
				input.putScalar(new int[]{ miniBatchIndex, styleIndexColumn, characterTimeStep }, 1.0 );

				mask.putScalar(new int[]{ miniBatchIndex, characterTimeStep }, 1.0);

				//		labels -> set the column index for the next character id-index -> at the current timestep (c)
				labels.putScalar(new int[]{ miniBatchIndex, nextCharIdx, characterTimeStep }, 1.0);

				currChar = nextChar;
				currCharIdx = nextCharIdx;
			} // for all of the characters -> write to a vector
		} // for
		
		examplesSoFar += miniBatchSize;
		INDArray mask2 = Nd4j.zeros(new int[]{ miniBatchSize, maxLength });
		Nd4j.copy(mask, mask2);
		return new DataSet(input,labels, mask, mask2);
	}

	public int totalExamples() {
		return numExamplesToFetch;
	}

	public int inputColumns() { return numCharacters + extraRatingColumns + beerDictionary.getBeerStyleCount(); }
	
	public int numCharacterColumns() { return numCharacters; }

	public int numRatings() { return 5; }

	public int numStyles() { return this.beerDictionary.getBeerStyleCount(); }

	public int totalOutcomes() {
		return numCharacters;
	}

	public void reset() {
		//System.out.println( "Resetting Beer Review Character Iterator..." );
		this.reviewReader.reset();
//		try {
//			this.reviewReader.init();
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
		examplesSoFar = 0;
	}

	public int batch() {
		return miniBatchSize;
	}

	public int cursor() {
		return examplesSoFar;
	}

	public int numExamples() {
		return numExamplesToFetch;
	}

	public void setPreProcessor(DataSetPreProcessor preProcessor) {
		throw new UnsupportedOperationException("Not implemented");
	}

	@Override
	public List<String> getLabels() {
		throw new UnsupportedOperationException("Not implemented");
	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException();
	}

	@Override
	public DataSetPreProcessor getPreProcessor() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean asyncSupported() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean resetSupported() {
		// TODO Auto-generated method stub
		return false;
	}

}
