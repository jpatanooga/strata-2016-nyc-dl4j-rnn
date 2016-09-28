package org.deeplearning4j.examples.rnn.beer;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.FileSystems;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.deeplearning4j.nn.multilayer.MultiLayerNetwork;
import org.deeplearning4j.util.ModelSerializer;
import twitter4j.ResponseList;
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
	BeerReviewCharacterIterator iter = null;
	MultiLayerNetwork net = null;
	Random rng = new Random();
	double temperature = 0;
	
	String pathToLSTMModel = "";
	
	ArrayList<String> folksToRespondTo = new ArrayList<String>();
	
	public static void main( String[] args ) throws Exception {
		
		LagerBot bot = new LagerBot();
		bot.init();

		while (true) {
			
			// hard coded spin loop? bad programming!
			
			String tweet = bot.generateReview();
			 
			System.out.println("Tweeting: " + tweet );
			bot.postReview( tweet );
			
			
			
			// 1 . check for mentions
			
			System.out.println( "Responding to Mentions..." );
			
			bot.scanTimeline();
			
			// 2. respond to all mentions
			
			// 3. post a beer review
		
			//String gen_review = bot.generateReview();
		
			long minInMS = 5 * 60 * 10;
			System.out.println( "Bot Sleepy Time... (10 min)" );
			Thread.sleep( minInMS );
		}
			
		
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
		
		
		this.loadLSTMModel();
		
	}
	
	public void loadLSTMModel() {
		String SEP = FileSystems.getDefault().getSeparator();
		String dataPath = System.getenv("BEER_REVIEW_PATH");
		File tempFile = new File(dataPath);
		assert(tempFile.exists() && !tempFile.isDirectory());
		String beerIdDataPath = dataPath + SEP + "beers_all.json";
		String reviewTrainingDataPath = dataPath + SEP + "reviews_top-train.json";

		char[] validCharacters = BeerReviewCharacterIterator.getDefaultCharacterSet();
		try {
			iter = new BeerReviewCharacterIterator(reviewTrainingDataPath, beerIdDataPath,
					Charset.forName("UTF-8"), 128, 0, 0, validCharacters, rng);
		} catch (IOException e) {
				System.exit(1);
		}

		String baseModelPath = System.getenv("MODEL_SAVE_PATH");
		ModelSaver saver = new ModelSaver(baseModelPath, 1);
		String modelSavePath = saver.getModelSavePath();
		System.out.println("Attempting to continue training from " + modelSavePath);
		try {
			net = ModelSerializer.restoreMultiLayerNetwork(modelSavePath);
		} catch (Exception e) {
			System.out.println("Failed to load model from " + modelSavePath);
			System.exit(1);
		}
	}
	
	public String generateReview() {
		
		String review = "";
		int rating = rng.nextInt(5) + 1	;
		
		// TODO: generate review from model here DAVE
		String[] reviews = SampleGeneratorListener.sampleBeerRatingFromNetwork(net, iter, rng, temperature, 2000, 1,
																		2, rating, rating, rating, rating, rating);
		review = reviews[0];
		System.out.println("************************************\n");
		
		if (120 < review.length()) {
			review = review.substring(0, 120);
		}
		
		return review;
		
	}
	
	public void scanTimeline() throws TwitterException {
		
		System.out.println( "Scan Timeline..." );
		
		this.folksToRespondTo.clear();
		
		ResponseList<Status> tweetsAtUs = this.twitter.getMentionsTimeline();
		
		for ( int x = 0; x < tweetsAtUs.size(); x++ ) {
		
			System.out.println( tweetsAtUs.get( x ).getUser().getScreenName() + " tweeted at us: " + tweetsAtUs.get( x ).getText() );
			
			String replyTo = tweetsAtUs.get( x ).getUser().getScreenName();
			
			String replyTweet = "@" + replyTo + " " + this.generateReview();
			
			this.postReview( replyTweet );
			
		}
		
		
		
		/*
		// The factory instance is re-useable and thread safe.
	    //Twitter twitter = TwitterFactory.getSingleton();
	    List<Status> statuses = this.twitter.getHomeTimeline();
	    System.out.println("Showing home timeline.");
	    for (Status status : statuses) {
	        System.out.println(status.getUser().getName() + ":" +
	                           status.getText());
	    }	
	    */	
		
	}
	
	public void postReview( String review ) throws TwitterException {

		Status status = twitter.updateStatus( review );
	    System.out.println("Successfully updated the status to [" + status.getText() + "].");

		
	}

}
