package org.deeplearning4j.examples.rnn.json;

import static org.junit.Assert.*;

import java.io.IOException;

import org.deeplearning4j.examples.rnn.beer.schema.json.BeerReview;
import org.deeplearning4j.examples.rnn.beer.schema.json.BeerReviewReader;
import org.junit.Test;

public class TestBeerReviewReader {

	@Test
	public void testReader() throws IOException {
		//fail("Not yet implemented");
		
		String pathToTestData = "/Users/josh/Documents/Talks/2016/Strata_NYC/data/beer/reviews_top-test.json";
		
		BeerReviewReader reader = new BeerReviewReader( pathToTestData );
		reader.init();
		
		for ( int x = 0; x < 1000; x++ ) {
		
			BeerReview b = reader.getNextReview();
		
			System.out.println( b.text );

		}
		
	}

}
