package org.deeplearning4j.examples.rnn.beer;

import java.util.List;

import twitter4j.Status;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.conf.ConfigurationBuilder;


/**
 * Setup  timer to scan timeline
 * 
 * 
 * 
 * 
 * 
 * @author josh
 *
 */
public class LagerBot {
	
	Twitter twitter = null;
	
	public static void main( String[] args ) throws Exception {
		
		LagerBot bot = new LagerBot();
		bot.init();
		
		
		// Black buddin amber with two finger filzy wite csouss to fingers of frut as yeticoff-white frothy rete
		
		String gen_review = "the color a smakling, light ass, alcosemend vet. Slight smellly sligttren, althe notcate floragiess two"; 
				//"Bringly, pour. Clear deep ambery amber. Smellis taste-and smerty. Loods but me towherfeam of its one ";
		
		bot.postReview(gen_review);
		
		//bot.scanTimeline();
		
	}
	
	public void init() throws TwitterException {
		
		ConfigurationBuilder cb = new ConfigurationBuilder();
		cb.setDebugEnabled(true)
		  .setOAuthConsumerKey("50S8DbHNUlryNiZ0PxRvp6GAC")
		  .setOAuthConsumerSecret("eMLgmvUyjd9hme7i7xTs4uzxk5Qm3tBi7mhtIQcMMjMl41tqi0")
		  .setOAuthAccessToken("776810737935257601-OtcUXTM7XDGthO8Yf194VM6MNAfHeWx")
		  .setOAuthAccessTokenSecret("VP2QqbSoecvK5vgQr7ySLBI0NoQPQ7oPXZhVOIdlPy7Fp");
		TwitterFactory tf = new TwitterFactory(cb.build());
		this.twitter = tf.getInstance();		
		
		//String review 
		
		
	}
	
	public void scanTimeline() throws TwitterException {
		
		// The factory instance is re-useable and thread safe.
	    //Twitter twitter = TwitterFactory.getSingleton();
	    List<Status> statuses = this.twitter.getHomeTimeline();
	    System.out.println("Showing home timeline.");
	    for (Status status : statuses) {
	        System.out.println(status.getUser().getName() + ":" +
	                           status.getText());
	    }		
		
	}
	
	public void postReview( String review ) throws TwitterException {

		Status status = twitter.updateStatus( review );
	    System.out.println("Successfully updated the status to [" + status.getText() + "].");

		
	}

}
