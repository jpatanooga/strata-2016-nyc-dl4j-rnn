package org.deeplearning4j.examples.rnn.json;

import static org.junit.Assert.*;

import java.io.IOException;

import org.deeplearning4j.examples.rnn.beer.schema.json.BeerDictionary;
import org.deeplearning4j.examples.rnn.beer.schema.json.BeerReview;
import org.deeplearning4j.examples.rnn.beer.schema.json.BeerReviewReader;
import org.deeplearning4j.examples.rnn.beer.schema.json.GroupedBeerDictionary;
import org.junit.Test;

import com.fasterxml.jackson.core.JsonProcessingException;

public class TestBeerDictionary {

	@Test
	public void test() throws JsonProcessingException, IOException {

		String pathToTestData = "/Users/josh/Documents/Talks/2016/Strata_NYC/data/beer/beers_all.json";
		
		BeerDictionary beerDictionary = new BeerDictionary();
		
		beerDictionary.loadBeerEntries( pathToTestData );
		
		beerDictionary.printBeerStyleStats();
		
		int index = beerDictionary.lookupBeerStyleIndex( "foo" );
		assertEquals( -1, index );
		
		int index2 = beerDictionary.lookupBeerStyleIndex( "Kvass" );
		assertEquals( 103, index2 );
		
		/*
		int c = reader.countReviews();
		
		System.out.println("found reviews: " + c);
		
		reader.init();
		
		for ( int x = 0; x < 10; x++ ) {
		
			BeerReview b = reader.getNextReview();
		
			System.out.println("Review: " + b.text );
			System.out.println("Review: " + b.rating_palate + "\n" );

		}
		*/
			
	
	
	
	}

	
	
	
	
	@Test
	public void testBeerDictionaryGrouped() throws JsonProcessingException, IOException {

		String pathToTestData = "/Users/josh/Documents/Talks/2016/Strata_NYC/data/beer/beers_all.json";
		
		BeerDictionary beerDictionary = new GroupedBeerDictionary();
		
		beerDictionary.loadBeerEntries( pathToTestData );
		
		beerDictionary.printBeerStyleStats();
		
		int index = beerDictionary.lookupBeerStyleIndex( "foo" );
		assertEquals( -1, index );
		
		int index2 = beerDictionary.lookupBeerStyleIndex( "Kvass" );
		assertEquals( -1, index2 );

		int index3 = beerDictionary.lookupBeerStyleIndex( "stout" );
		assertEquals( 1, index3 );
		
		assertEquals( 5, beerDictionary.getBeerStyleCount() );
		
		/*
		int c = reader.countReviews();
		
		System.out.println("found reviews: " + c);
		
		reader.init();
		
		for ( int x = 0; x < 10; x++ ) {
		
			BeerReview b = reader.getNextReview();
		
			System.out.println("Review: " + b.text );
			System.out.println("Review: " + b.rating_palate + "\n" );

		}
		*/
			
	
	
	
	}	
	
	
	
}
