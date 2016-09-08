package org.deeplearning4j.examples.rnn.beer.schema.json;

/*
 * 
  "3": {
   "name": "Cactus Queen IPA", 
   "style": "American IPA", 
   "id": 3, 
   "abv": null, 
   "brewer": 2
  }, 
  "4": {
   "name": "Wildcatter's Crude Stout", 
   "style": "American Stout", 
   "id": 4, 
   "abv": null, 
   "brewer": 2
  }, 
  "5": {
   "name": "Amber", 
   "style": "Vienna Lager", 
   "id": 5, 
   "abv": 4.5, 
   "brewer": 3
  }, 
 * 
 */
public class BeerStyle {
	
	public String style = null;
	//public String id = "";
	public int count = 0;
	public int index = 0;

}
