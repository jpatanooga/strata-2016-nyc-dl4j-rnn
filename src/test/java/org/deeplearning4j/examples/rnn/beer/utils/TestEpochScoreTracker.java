package org.deeplearning4j.examples.rnn.beer.utils;

import static org.junit.Assert.*;

import org.junit.Test;

public class TestEpochScoreTracker {

	@Test
	public void test() {
		
		EpochScoreTracker tracker = new EpochScoreTracker();
		
		tracker.addScore( 0.9 );
		tracker.addScore( 0.8 );
		tracker.addScore( 0.7 );
		
		System.out.println( tracker.avgScore() );
		
		System.out.println( "window: "  + tracker.scoreChangeOverWindow() );
		
		System.out.println( "first: " +  tracker.firstScore );
		
		tracker.debug();
		
		System.out.println( "avg improvement: " +  tracker.averageLossImprovementPerEpoch() );
		
		tracker.setTargetLossScore(0.2);
		
		System.out.println( "remaining epochs: " +  tracker.computeProjectedEpochsRemainingToTargetLossScore() );
		
		tracker.addScore( 0.71 );
		
		tracker.addScore( 0.6 );
		
		System.out.println( "avg improvement: " +  tracker.averageLossImprovementPerEpoch() );
		
		
		System.out.println( "remaining epochs: " +  tracker.computeProjectedEpochsRemainingToTargetLossScore() );
		
		
	}
	
	@Test
	public void testNoEpochs() {
		
		EpochScoreTracker tracker = new EpochScoreTracker();
		
//		tracker.addScore( 0.9 );
	//	tracker.addScore( 0.8 );
		//tracker.addScore( 0.7 );
		
		System.out.println( tracker.avgScore() );
		
		System.out.println( "window: "  + tracker.scoreChangeOverWindow() );
		
		System.out.println( "first: " +  tracker.firstScore );
		
		tracker.debug();
		
		System.out.println( "avg improvement: " +  tracker.averageLossImprovementPerEpoch() );
		
		
	}
	
	@Test
	public void testSingleEpochs() {
		
		EpochScoreTracker tracker = new EpochScoreTracker();
		
		tracker.addScore( 0.9 );
	//	tracker.addScore( 0.8 );
		//tracker.addScore( 0.7 );
		
		System.out.println( tracker.avgScore() );
		
		System.out.println( "window: "  + tracker.scoreChangeOverWindow() );
		
		System.out.println( "first: " +  tracker.firstScore );
		
		tracker.debug();
		
		System.out.println( "avg improvement: " +  tracker.averageLossImprovementPerEpoch() );
		
		
	}	
	

}
