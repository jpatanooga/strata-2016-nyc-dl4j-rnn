package org.deeplearning4j.examples.rnn.beer.utils;

import static org.junit.Assert.*;

import org.junit.Test;

public class TestEpochScoreTracker {

	@Test
	public void test() {
		
		EpochScoreTracker tracker = new EpochScoreTracker();
		
		tracker.addScore( 0.1 );
		tracker.addScore( 0.2 );
		tracker.addScore( 0.3 );
		
		System.out.println( tracker.avgScore() );
		
		System.out.println( "window: "  + tracker.scoreChangeOverWindow() );
		
		System.out.println( "first: " +  tracker.firstScore );
		
		tracker.debug();
		
	}

}
