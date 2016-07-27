package org.deeplearning4j.examples.rnn.beer.schema.json;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class BeerReviewReader {

	public String strDataPath = "";
	
	Iterator<JsonNode> reviewJsonElementIterator = null;
	ObjectMapper jacksonJSONObjectMapper = null;
	
	public BeerReviewReader(String strJSONDataPath) {
		
		this.strDataPath = strJSONDataPath;
		
	}
	
	public void init() throws IOException {
		

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
	
	public BeerReview getNextReview() throws IOException {
		
		JsonNode beer_review = this.reviewJsonElementIterator.next();
		BeerReview b = this.jacksonJSONObjectMapper.readValue( beer_review.toString(), BeerReview.class );
			
			
		//	System.out.println("review: " + b.text );
		//	System.out.println("\tuser: " + b.user );
		//	System.out.println("\trating overall: " + b.rating_overall );
			

		return b;
		
	}
	
	public void close() {
		
		//this
		
	}
	
	
	
}
