package org.deeplearning4j.examples.rnn.json;

import static org.junit.Assert.*;

import org.deeplearning4j.examples.rnn.beer.BeerReviewCharacterIterator;
import org.deeplearning4j.examples.rnn.beer.LSTMBeerReviewModelingExample;
import org.deeplearning4j.examples.rnn.beer.utils.NDArrayUtils;
import org.junit.Test;
import org.nd4j.linalg.dataset.DataSet;

public class TestBeerReviewCharacterIterator {

	@Test
	public void test() throws Exception {
		String pathToTestData = "/Users/josh/Documents/Talks/2016/Strata_NYC/data/beer/reviews_top-test.json";

		
		BeerReviewCharacterIterator iter = LSTMBeerReviewModelingExample.getBeerReviewIterator(10, 100, 3500);
		
		while (iter.hasNext()) {
			
			DataSet ds = iter.next( 10 );
			
		}
		
		//NDArrayUtils.debug3D_Nd4J_Input( ds.getFeatures(), 1, iter.inputColumns(), 100 );
		
		
	}

}
