package org.deeplearning4j.examples.rnn.beer.schema.json;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Iterator;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class GroupedBeerDictionary extends BeerDictionary {

	public int filteredBeers = 0;
	
	/**
	 * Adds the style to the map, updates counts
	 * 
	 * @param beer
	 * @return returns the beer style index
	 */
	@Override
	public int addBeerStyleToMap( Beer beer ) {
		
		// Porter, Ale, Lager, Stout, IPA
		
		String keyStyleParsed = this.parseGroupedStyle( beer.style ); //beer.style.toLowerCase().trim();
		
		BeerStyle bs = null;
		
		if (null == keyStyleParsed) {
			
			this.filteredBeers++;
		
		} else if ( this.beerMap.containsKey( keyStyleParsed ) ) {
			
			bs = this.beerMap.get( keyStyleParsed );
			bs.count++;
			this.beerMap.put( keyStyleParsed, bs );
			
		} else {
			
			bs = new BeerStyle();
			bs.style = keyStyleParsed; //beer.style; // dont parse yet
			bs.count = 1;
			bs.index = this.beerMap.size();
			
			this.beerMap.put( keyStyleParsed, bs );
			
		}
				

		if (null != bs) {

			return bs.index;
			
		}
		
		return -1;
		
	}	
	
	public static String parseGroupedStyle(String style) {
		
		// Porter, Ale, Lager, Stout, IPA
		String[] styleGroups = { "porter", "ale", "lager", "stout", "ipa" };
		
		String trimmedStyle = style.toLowerCase().trim();
		
		for (String key: styleGroups) {
			
			int index = trimmedStyle.indexOf(key);
			if (index > -1) {
				return key;
			}
			
		}
		
		return null;
	}
	
}
