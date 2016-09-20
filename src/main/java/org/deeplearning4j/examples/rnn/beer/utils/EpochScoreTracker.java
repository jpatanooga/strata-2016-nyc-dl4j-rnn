package org.deeplearning4j.examples.rnn.beer.utils;

import java.util.ArrayList;

public class EpochScoreTracker {
	
	public double firstScore = 0.0;
	
	ArrayList<Double> scores = new ArrayList<>();
	int windowSize = 5; // default
	public double targetLossScore = -1.0;
	int totalEpochsTracked = 0;
	
	public EpochScoreTracker() {
		
	}

	public EpochScoreTracker(int size) {
		
		this.windowSize = size;
		
	}
	
	public void setTargetLossScore(double score) {
		this.targetLossScore = score;
	}
	
	public void debug() {
		
		for ( int x = 0; x < this.scores.size(); x++ ) {
			
			System.out.println( this.scores.get(x) );
			
		}
		
	}
	
	
	public void addScore(double score) {
		
		if ( this.scores.size() < 1) {
			this.firstScore = score;
		}
		
		// add it to the front
		this.scores.add( 0, score );
		while ( this.scores.size() > this.windowSize) {
			
			this.scores.remove( this.scores.size() - 1 ); // remove the tail
			
		}
		
		this.totalEpochsTracked++;
		
	}
	
	public double avgScore() {
		
		double sum = 0.0;
		for ( int x = 0; x < this.scores.size(); x++ ) {
			
			sum += this.scores.get( x );
			
		}
		
		return sum / this.scores.size();
		
	}
	
	public double scoreChangeOverWindow() {
		
		if (this.totalEpochsTracked == 0) {
			return 0.0;
		}

		if (this.totalEpochsTracked == 1) {
			return this.firstScore;
		}
		
		
		double first = this.firstScore; //this.scores.get(0);
		double last = this.scores.get( 0 ); // this.scores.size() - 1 );
		/*
		System.out.println( "size: " + this.scores.size() );
		
		System.out.println( "first: " + first );
		System.out.println( "last: " + last );
*/
		return first - last;
			
		
	}
	
	public double averageLossImprovementPerEpoch() {
		
		double lossChange = this.scoreChangeOverWindow();
		
		if (0 == this.totalEpochsTracked) {
			return 0.0;
		}

		if (1 == this.totalEpochsTracked) {
			return lossChange;
		}
		
		
		return lossChange / (this.totalEpochsTracked - 1);
		
		
	}
	
	public double computeProjectedEpochsRemainingToTargetLossScore() {
		
		if (this.totalEpochsTracked < 5) {
			// too few
			return 0.0;
		}
		
		
		double avgLossPerEpoch = this.averageLossImprovementPerEpoch();
		double lastLossScore = this.scores.get( 0 );
		
		return lastLossScore / avgLossPerEpoch;
		
		
	}

}
