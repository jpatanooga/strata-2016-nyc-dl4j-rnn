package org.deeplearning4j.examples.rnn.beer.schema.json;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * 
 * ,"rating_overall":4.0,
 * "rating_aroma":5.0,
 * "rating_taste":4.0,
 * "rating_appearance":5.0,
 * "rating_palate":4.0,
 * "user":"DrDemento456"
 * }
 * 
 * 
 * @author josh
 *
 */
//@JsonIgnoreProperties
public class BeerReview {
	
	public String text = "";
	public float rating_overall = 0.0f;
	public float rating_taste = 0.0f;
	public float rating_appearance = 0.0f;
	public float rating_palate = 0.0f;
	public float rating_aroma = 0.0f;
	public String user = "";
	
	// todo: added beer ID
	public String beer_id = "0";
	
	public Beer beer = null;

}
