package org.deeplearning4j.examples.rnn.json;

import static org.junit.Assert.*;

import org.deeplearning4j.examples.rnn.beer.BeerReviewCharacterIterator;
import org.deeplearning4j.examples.rnn.beer.LSTMBeerReviewModelingExample;
import org.junit.Test;

public class TestBeerReviewCharacterIterator {

	@Test
	public void test() throws Exception {
		String pathToTestData = "/Users/josh/Documents/Talks/2016/Strata_NYC/data/beer/reviews_top-test.json";

		
		BeerReviewCharacterIterator iter = LSTMBeerReviewModelingExample.getBeerReviewIterator(10, 100, 20);
		
		iter.next( 10 );
		
		
	}

}
