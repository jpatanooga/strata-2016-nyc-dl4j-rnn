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
		//String pathToTestData = "/Users/josh/Documents/Talks/2016/Strata_NYC/data/beer/reviews_top-test.json";

		String pathToBeerData = "/Users/josh/Documents/Talks/2016/Strata_NYC/data/beer/beers_all.json";
		
		// String pathToData = "/Users/josh/Documents/Talks/2016/Strata_NYC/data/beer/simple_reviews_debug.json";
		String pathToData = "/Users/josh/Documents/Talks/2016/Strata_NYC/data/beer/reviews_core-train.json";
		
		int examplesPerEpoch = 240000;
		int miniBatchSize = 40;
		
		BeerReviewCharacterIterator iter = LSTMBeerReviewModelingExample.getBeerReviewIterator( miniBatchSize, 100, examplesPerEpoch, pathToData, pathToBeerData);
		
		int count = 0;
		while (iter.hasNext()) {
			
			DataSet ds = iter.next( miniBatchSize );
			
			count += miniBatchSize;
			
			System.out.println( "Next " + count );
			
			if (count > 100) {
				break;
			}
			
		}
		
		/*
		iter.reset();
		
		count = 0;
		
		while (iter.hasNext()) {
			
			DataSet ds = iter.next( miniBatchSize );
			
			count += miniBatchSize;
			
			System.out.println( "Next " + count );
			
		}
		*/
		
		//NDArrayUtils.debug3D_Nd4J_Input( ds.getFeatures(), 1, iter.inputColumns(), 100 );
		
		
	}

}
