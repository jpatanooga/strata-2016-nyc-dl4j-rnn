package org.deeplearning4j.examples.rnn.beer;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Random;

import org.deeplearning4j.datasets.iterator.DataSetIterator;
import org.deeplearning4j.examples.rnn.beer.schema.json.BeerReview;
import org.deeplearning4j.examples.rnn.beer.schema.json.BeerReviewReader;
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
	private int exampleLength;
	private int miniBatchSize;
	private int numExamplesToFetch;
	private int examplesSoFar = 0;
	private Random rng;
	private final int numCharacters;
	private final boolean alwaysStartAtNewLine;
	
	// account for beer review rating columns
	int extraRatingColumns = 5;
	
	BeerReviewReader reviewReader = null;
	
	public BeerReviewCharacterIterator(String path, int miniBatchSize, int exampleSize, int numExamplesToFetch ) throws IOException {
		this(path,Charset.defaultCharset(),miniBatchSize,exampleSize,numExamplesToFetch,getDefaultCharacterSet(), new Random(),true);
	}
	
	/**
	 * @param textFilePath Path to text file to use for generating samples
	 * @param textFileEncoding Encoding of the text file. Can try Charset.defaultCharset()
	 * @param miniBatchSize Number of examples per mini-batch
	 * @param exampleLength Number of characters in each input/output vector
	 * @param numExamplesToFetch Total number of examples to fetch (must be multiple of miniBatchSize). Used in hasNext() etc methods
	 * @param validCharacters Character array of valid characters. Characters not present in this array will be removed
	 * @param rng Random number generator, for repeatability if required
	 * @param alwaysStartAtNewLine if true, scan backwards until we find a new line character (up to MAX_SCAN_LENGTH in case
	 *  of no new line characters, to avoid scanning entire file)
	 * @throws IOException If text file cannot  be loaded
	 */
	public BeerReviewCharacterIterator(String textFilePath, Charset textFileEncoding, int miniBatchSize, int exampleLength,
			int numExamplesToFetch, char[] validCharacters, Random rng, boolean alwaysStartAtNewLine ) throws IOException {
		if( !new File(textFilePath).exists()) throw new IOException("Could not access file (does not exist): " + textFilePath);
		if(numExamplesToFetch % miniBatchSize != 0 ) throw new IllegalArgumentException("numExamplesToFetch must be a multiple of miniBatchSize");
		if( miniBatchSize <= 0 ) throw new IllegalArgumentException("Invalid miniBatchSize (must be >0)");
		this.validCharacters = validCharacters;
		this.exampleLength = exampleLength;
		this.miniBatchSize = miniBatchSize;
		this.numExamplesToFetch = numExamplesToFetch;
		this.rng = rng;
		this.alwaysStartAtNewLine = alwaysStartAtNewLine;
		
		//Store valid characters is a map for later use in vectorization
		charToIdxMap = new HashMap<>();
		for( int i=0; i<validCharacters.length; i++ ) charToIdxMap.put(validCharacters[i], i);
		numCharacters = validCharacters.length;
		
		//Load file and convert contents to a char[] 
		boolean newLineValid = charToIdxMap.containsKey('\n');
		
		/*
		List<String> lines = Files.readAllLines(new File(textFilePath).toPath(),textFileEncoding);
		
		int maxSize = lines.size();	//add lines.size() to account for newline characters at end of each line 
		for( String s : lines ) { 
			maxSize += s.length();
		}
		
		// create array to contain all lines -> characters 
		char[] characters = new char[maxSize];
		
		// for each line, convert into array
		int currIdx = 0;
		for( String s : lines ){
			char[] thisLine = s.toCharArray();
			for( int i=0; i<thisLine.length; i++ ){
				if( !charToIdxMap.containsKey(thisLine[i]) ) continue;
				characters[currIdx++] = thisLine[i];
			}
			if(newLineValid) characters[currIdx++] = '\n';
		}
		
		if( currIdx == characters.length ){
			fileCharacters = characters;
		} else {
			fileCharacters = Arrays.copyOfRange(characters, 0, currIdx);
		}
		if( exampleLength >= fileCharacters.length ) throw new IllegalArgumentException("exampleLength="+exampleLength
				+" cannot exceed number of valid characters in file ("+fileCharacters.length+")");
		
		int nRemoved = maxSize - fileCharacters.length;
		*/
		
		// TODO: init reader
		this.reviewReader = new BeerReviewReader( textFilePath );
		
		int c = this.reviewReader.countReviews();
		
		System.out.println("found reviews: " + c);
		
		
		this.reviewReader.init();
		
		
	//	System.out.println("Loaded and converted file: " + fileCharacters.length + " valid characters of "
	//	+ maxSize + " total characters (" + nRemoved + " removed)");
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
	
	public char convertIndexToCharacter( int idx ){
		return validCharacters[idx];
	}
	
	public int convertCharacterToIndex( char c ){
		return charToIdxMap.get(c);
	}
	
	public char getRandomCharacter(){
		return validCharacters[(int) (rng.nextDouble()*validCharacters.length)];
	}

	public boolean hasNext() {
		return examplesSoFar + miniBatchSize <= numExamplesToFetch;
	}

	public DataSet next() {
		return next(miniBatchSize);
	}

	private char[] convertReviewToCharacters( BeerReview br ) {
		
		
		
		// for each line, convert into array
		int currIdx = 0;
		char[] thisLine = br.text.toCharArray(); //s.toCharArray();
		
		char[] characters = new char[ thisLine.length ];
		
		for( int i=0; i< thisLine.length; i++ ){
			if( !charToIdxMap.containsKey(thisLine[i]) ) {
				continue;
			}
			characters[currIdx++] = thisLine[i];
		}
		
		return characters;
		
	}
	

	/**
	 * TODO: how do we handle this for different type review hints per mini-batch entry?
	 * 
	 * @param ndArray
	 * @param rating_overall
	 * @param rating_taste
	 * @param rating_appearance
	 * @param rating_palate
	 * @param rating_aroma
	 */
	public void setReviewHints( INDArray ndArray, float rating_overall, float rating_taste, float rating_appearance, float rating_palate, float rating_aroma) {
		/*
		ndArray.putScalar(new int[]{ miniBatchIndex, staticColumnBaseOffset, characterTimeStep }, rating_appearance );
		ndArray.putScalar(new int[]{ miniBatchIndex, staticColumnBaseOffset + 1, characterTimeStep }, rating_aroma );
		ndArray.putScalar(new int[]{ miniBatchIndex, staticColumnBaseOffset + 2, characterTimeStep }, rating_overall );
		ndArray.putScalar(new int[]{ miniBatchIndex, staticColumnBaseOffset + 3, characterTimeStep }, rating_palate );
		ndArray.putScalar(new int[]{ miniBatchIndex, staticColumnBaseOffset + 4, characterTimeStep }, rating_taste );
		*/
		
	}
	
	/**
	 * main method to produce vectors to modeling algorithm / workflow
	 * 
	 * 
	 * TODO:
	 * 		-	add columns for: { 
					public float rating_overall = 0.0f;
					public float rating_taste = 0.0f;
					public float rating_appearance = 0.0f;
					public float rating_palate = 0.0f;
					public float rating_aroma = 0.0f;
	 * 
	 *		-	 
	 * 
	 * 
	 */
	public DataSet next(int miniBatchSize) {
		
		if( examplesSoFar+miniBatchSize > numExamplesToFetch ) {
			throw new NoSuchElementException();
		}
		
		
		
		int inputColumnCount = numCharacters + extraRatingColumns;
		int outputColumnCount = numCharacters;
		
		//Allocate space: { mini-batch size, number columns, number timesteps }
		INDArray input = Nd4j.zeros(new int[]{ miniBatchSize, inputColumnCount, exampleLength });
		INDArray labels = Nd4j.zeros(new int[]{ miniBatchSize, outputColumnCount, exampleLength });
		
		// TODO: at this point we probably need to consider moving the beer review into the fileCharacters array
		
		
//		int maxStartIdx = fileCharacters.length - exampleLength;
		
		//Randomly select a subset of the file. No attempt is made to avoid overlapping subsets
		// of the file in the same minibatch
		for( int miniBatchIndex = 0; miniBatchIndex < miniBatchSize; miniBatchIndex++ ){
			
			// we have to get each review separately
			
			BeerReview br = null;
			
			try {
				br = this.reviewReader.getNextReview();
				
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			//System.out.println( br.text );
			
			fileCharacters = this.convertReviewToCharacters( br );
			
			if (fileCharacters.length < 10) {
				System.err.println( "Small review text length! [count: " + this.reviewReader.getCount() + "]" );
				System.err.println( "Text: " + br.text );
				
			}
			
			int startIdx = 0; //(int) (rng.nextDouble()*maxStartIdx);
			int endIdx = startIdx + exampleLength;
			if (endIdx > fileCharacters.length - 1) {
				endIdx = fileCharacters.length - 1;
			}
			//int scanLength = 0;
/*			if(alwaysStartAtNewLine){
				while(startIdx >= 1 && fileCharacters[startIdx-1] != '\n' && scanLength++ < MAX_SCAN_LENGTH ){
					startIdx--;
					endIdx--;
				}
			}
	*/		
			//System.out.println( "debug> startIdx: " + startIdx + ", endIdx: " + endIdx );
			
			int currCharIdx = charToIdxMap.get( fileCharacters[ startIdx ] );	//Current input
			int characterTimeStep = 0;
			
			for ( int j = startIdx + 1; j <= endIdx; j++, characterTimeStep++ ){
				
				int nextCharIdx = charToIdxMap.get( fileCharacters[ j ] );		//Next character to predict
				// mini-batch-index, column-index, timestep-index -> 1.0 
				// for this mini batch example
				//		input -> set the column index for the character id-index -> at the current timestep (c)
				input.putScalar(new int[]{ miniBatchIndex, currCharIdx, characterTimeStep }, 1.0);
				//		labels -> set the column index for the next character id-index -> at the current timestep (c)
				labels.putScalar(new int[]{ miniBatchIndex, nextCharIdx, characterTimeStep }, 1.0);
				currCharIdx = nextCharIdx;
				
				// note: effectively mapping { input -> output }, saying at this timestep when we see X character in the input, the output should show Y character
				
				//System.out.println( fileCharacters[ j - 1 ] + " -> " + currCharIdx );
				
				// setup static columns
				

				
				// TODO: consider --- do we put this only in the input or the output too?
				
				// input
				
				int staticColumnBaseOffset = numCharacters;
				
				input.putScalar(new int[]{ miniBatchIndex, staticColumnBaseOffset, characterTimeStep }, br.rating_appearance );
				input.putScalar(new int[]{ miniBatchIndex, staticColumnBaseOffset + 1, characterTimeStep }, br.rating_aroma );
				input.putScalar(new int[]{ miniBatchIndex, staticColumnBaseOffset + 2, characterTimeStep }, br.rating_overall );
				input.putScalar(new int[]{ miniBatchIndex, staticColumnBaseOffset + 3, characterTimeStep }, br.rating_palate );
				input.putScalar(new int[]{ miniBatchIndex, staticColumnBaseOffset + 4, characterTimeStep }, br.rating_taste );
				
				// TODO: need to handle Beer ID
				
				// TODO: do we handle user ID?
				
			} // for all of the characters -> write to a vector
			
			
		} // for
		
		examplesSoFar += miniBatchSize;
		return new DataSet(input,labels);
	}

	public int totalExamples() {
		return numExamplesToFetch;
	}

	public int inputColumns() {
		return numCharacters + extraRatingColumns;
	}

	public int totalOutcomes() {
		return numCharacters;
	}

	public void reset() {
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

}
