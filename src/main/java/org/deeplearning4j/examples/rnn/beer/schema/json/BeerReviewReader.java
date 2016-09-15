package org.deeplearning4j.examples.rnn.beer.schema.json;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class BeerReviewReader {

	public String strDataPath = "";
	int count = 0;
	
	Iterator<JsonNode> reviewJsonElementIterator = null;
	ObjectMapper jacksonJSONObjectMapper = null;
	
	public BeerReviewReader(String strJSONDataPath) {
		
		this.strDataPath = strJSONDataPath;
		
	}
	
	public int getCount() {
		return count;
	}
	
	public void init() throws IOException {
		
	//	System.out.println( "initializing beer review reader..." );

		//String pathToTestData = "/Users/josh/Documents/Talks/2016/Strata_NYC/data/beer/reviews_top-test.json";
		
		//org.json.
		
		//List<BeerReview> beerReviews = new ArrayList<BeerReview>();
		
		//byte[] jsonData = Files.readAllBytes(Paths.get( pathToTestData ));
		//InputStream in = Text
		BufferedReader br = new BufferedReader(new InputStreamReader( new FileInputStream( this.strDataPath )));
		
		this.jacksonJSONObjectMapper = new ObjectMapper();
		
		
//		this.jacksonJSONObjectMapper.readTree( br );
		
		com.fasterxml.jackson.databind.JsonNode rootNode = this.jacksonJSONObjectMapper.readTree( br ); //this.jacksonJSONObjectMapper.readTree( jsonData );
		
		Iterator<JsonNode> root_iter = rootNode.elements();
		
		JsonNode list = root_iter.next();
		
		this.reviewJsonElementIterator = list.elements();
		
		
				
		
	}
	
	public int countReviews() throws JsonProcessingException, IOException {
		
		BufferedReader br = new BufferedReader(new InputStreamReader( new FileInputStream( this.strDataPath )));
		
		ObjectMapper countJacksonJSONObjectMapper = new ObjectMapper();
		
		
//		this.jacksonJSONObjectMapper.readTree( br );
		
		com.fasterxml.jackson.databind.JsonNode rootNode = countJacksonJSONObjectMapper.readTree( br ); //this.jacksonJSONObjectMapper.readTree( jsonData );
		
		Iterator<JsonNode> root_iter = rootNode.elements();
		
		JsonNode list = root_iter.next();
		
		Iterator<JsonNode> iterTmp = reviewJsonElementIterator = list.elements();	
		int countTmp = 0;
		
		while (iterTmp.hasNext()) {
			
			countTmp++;
			//JsonNode beer_review = 
			iterTmp.next();
		}
		
		return countTmp;
		
	}
	
	public BeerReview getNextReview() throws IOException {
		
		if (false == this.reviewJsonElementIterator.hasNext()) {
			return null;
		}
		
		JsonNode beer_review = this.reviewJsonElementIterator.next();
		BeerReview b = this.jacksonJSONObjectMapper.readValue( beer_review.toString(), BeerReview.class );
		
		//this.jacksonJSONObjectMapper.
		
		this.count++;
		
		while (b.text.length() < 1) {
			System.err.println( "Skipped empty review at count: " + this.count );
			beer_review = this.reviewJsonElementIterator.next();
			b = this.jacksonJSONObjectMapper.readValue( beer_review.toString(), BeerReview.class );
			this.count++;
		}
		
		
			
		//	System.out.println("review: " + b.text );
		//	System.out.println("\tuser: " + b.user );
		//	System.out.println("\trating overall: " + b.rating_overall );
			

		return b;
		
	}
	
	/**
	 * Hack to get this going
	 * 
	 * @return
	 * @throws IOException
	 */
	public BeerReview getNextFilteredReview(GroupedBeerDictionary dict) throws IOException {
		
		if (false == this.reviewJsonElementIterator.hasNext()) {
			return null;
		}
		
		JsonNode beer_review = this.reviewJsonElementIterator.next();
		BeerReview b = this.jacksonJSONObjectMapper.readValue( beer_review.toString(), BeerReview.class );
		
		//this.jacksonJSONObjectMapper.
		
		this.count++;
		
		int styleIndex = dict.lookupBeerStyleIndexByBeerID( b.beer_id );
		
		while (b.text.length() < 1 || styleIndex < 0) {
		//	System.err.println( "Skipped empty/filtered review at count: " + this.count );
			beer_review = this.reviewJsonElementIterator.next();
			b = this.jacksonJSONObjectMapper.readValue( beer_review.toString(), BeerReview.class );
			styleIndex = dict.lookupBeerStyleIndexByBeerID( b.beer_id );
			this.count++;
		}
		
		
			
		//	System.out.println("review: " + b.text );
		//	System.out.println("\tuser: " + b.user );
		//	System.out.println("\trating overall: " + b.rating_overall );
			

		return b;
		
	}	
	
	private boolean passesFilter(String style) {
		
		String styleFiltererd = GroupedBeerDictionary.parseGroupedStyle( style );
		
		if ( null == styleFiltererd ) {
			return false;
		}
		
		return true;
		
	}
	
	public boolean hasNext() {
		
		return this.reviewJsonElementIterator.hasNext();
		
	}
	
	public void close() {
		
		//this
		
	}
	
	
	
}
