package org.deeplearning4j.examples.rnn.beer.schema.json;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Purpose: maps all of the beer styles in the beers_all.json file
 * 
 * 
 * @author josh
 *
 */
public class BeerDictionary {

	public String strDataPath = "";
	int count = 0;
	
	Iterator<JsonNode> beerJsonElementIterator = null;
	ObjectMapper jacksonJSONObjectMapper = null;	
	
	// map: { styleKey, BeerStyle }
	Map<String, BeerStyle> beerMap = new HashMap<>();
	
	// JOIN: br.beer_id -> beer.style -> beerStyle.index
	// MEANING: we need to pre-cache this join in the beer dictionary
	// SO: for each beer: link beer_id -> beer_style_index
	
	Map<String, Integer> beerIDToStyleIndexMap = new HashMap<>();
	
	
	// alternate index
	Map<String, String> beerIDToStyleMap = new HashMap<>();
	
	
	public void loadBeerEntries(String strPath) throws JsonProcessingException, IOException {
		
		this.strDataPath = strPath;
		
		BufferedReader br = new BufferedReader(new InputStreamReader( new FileInputStream( this.strDataPath )));
		
		this.jacksonJSONObjectMapper = new ObjectMapper();
				
		com.fasterxml.jackson.databind.JsonNode rootNode = this.jacksonJSONObjectMapper.readTree( br ); //this.jacksonJSONObjectMapper.readTree( jsonData );
		
		Iterator<JsonNode> root_iter = rootNode.elements();
		
		JsonNode list = root_iter.next();
		
		this.beerJsonElementIterator = list.elements();
				
		
		long beerCount = 0;
		
		while (this.beerJsonElementIterator.hasNext()) {
		//for (int x = 0; x < 10; x++ ) {
			
			JsonNode beer = this.beerJsonElementIterator.next();
			Beer b = this.jacksonJSONObjectMapper.readValue( beer.toString(), Beer.class );
			
			//System.out.println( b.style );
			
			// TODO: THIS IS BROKE
			
			//this.beerMapByID.put( b.id, b );
			//this.updateBeerStyleStats( b.id );
			int index = this.addBeerStyleToMap( b );
			
			if (-1 == index) {
				
			} else {
				this.mapBeerIDtoStyleIndex( b.id, index );
				this.mapBeerIDtoStyle( b.id, b.style );
			
				beerCount++;
			}
			
		}
		
		System.out.println( "Added " + beerCount + " to the Beer Dictionary(tm)..." );
		System.out.println( "Beer style count: " + this.beerMap.size() );
		System.out.println( "Beer ID Map count: " + this.beerIDToStyleIndexMap.size() );
		
		
	}
	
	public int getBeerStyleCount() {
		return this.beerMap.size();
	}
	
	
	/**
	 * Adds the style to the map, updates counts
	 * 
	 * @param beer
	 * @return returns the beer style index
	 */
	public int addBeerStyleToMap( Beer beer ) {
		
		String keyStyleParsed = beer.style.toLowerCase().trim();
		
		BeerStyle bs = null;
		
		if ( this.beerMap.containsKey( keyStyleParsed ) ) {
			
			bs = this.beerMap.get( keyStyleParsed );
			bs.count++;
			this.beerMap.put( keyStyleParsed, bs );
			
		} else {
			
			bs = new BeerStyle();
			bs.style = beer.style; // dont parse yet
			bs.count = 1;
			bs.index = this.beerMap.size();
			
			this.beerMap.put( keyStyleParsed, bs );
			
		}
				
		
		//this.beerMap.put(, value)
		return bs.index;
		
	}
	
	public void mapBeerIDtoStyleIndex(String beerID, int styleIndex) {
		
		this.beerIDToStyleIndexMap.put( beerID, styleIndex );
		
	}

	public void mapBeerIDtoStyle(String beerID, String style) {
		
		this.beerIDToStyleMap.put( beerID, style );
		
	}
	
	
	public int lookupBeerStyleIndex( String style ) {
		
		String keyStyleParsed = style.toLowerCase().trim();
		
		if ( this.beerMap.containsKey( keyStyleParsed ) ) {
			
			BeerStyle bs = this.beerMap.get( keyStyleParsed );
			return bs.index;
			
		}		

		return -1;
		
	}
	
	
	
	// JOIN: br.beer_id -> beer.style -> beerStyle.index
	// MEANING: we need to pre-cache this join in the beer dictionary
	// SO: for each beer: link beer_id -> beer_style_index
	
	public int lookupBeerStyleIndexByBeerID(String beerID ) {
		
		if (this.beerIDToStyleIndexMap.containsKey(beerID)) {
			
			return this.beerIDToStyleIndexMap.get( beerID );
		}
		
		// default
		return -1;
	}
	
	public String lookupBeerStyleByBeerID(String beerID ) {
		
		if (this.beerIDToStyleMap.containsKey(beerID)) {
			
			return this.beerIDToStyleMap.get( beerID );
		}
		
		// default
		return null;
	}	
	
	/*
	public void updateBeerStyleStats(String id) {
		
		String idTrimmed = id.trim().toLowerCase();
		
		if ( this.beerStyleStatsMap.containsKey( idTrimmed ) ) {
			
			Integer styleCount = this.beerStyleStatsMap.get(idTrimmed);
			this.beerStyleStatsMap.put( idTrimmed, styleCount + 1 );
			
		} else {
			
			this.beerStyleStatsMap.put(idTrimmed, 1);
			
		}
		
		
	}
	*/
	
	public void printBeerStyleStats() {
		
		System.out.println( "Beer Dictionary: Style Statistics -------------" );
		
		int count = 0;
		
		System.out.println( "beer style count: " + this.beerMap.size() );
		
		for (Map.Entry<String,BeerStyle> entry : this.beerMap.entrySet()) {
		
			String key_beerStyle_id = entry.getKey();
			
			BeerStyle beer_style = entry.getValue();
			  // do stuff
			
			//Beer beer = this.beerMapByID.get( key_beer_id );
			//Integer beer_stat = this.beerStyleStatsMap.get(beer.style.toLowerCase().trim());
			
			System.out.println( count + ". " + beer_style.style + " (index: " + beer_style.index + "): " + beer_style.count );
			
			count++;
			
		}	
		
		System.out.println( "Total Beer Styles: " + count );
		
		System.out.println( "Beer Dictionary: Style Statistics -------------" );
		
	}
/*	
	public Beer getBeerByID( String beer_id ) {
		
		return this.beerMapByID.get( beer_id );
		
	}
	*/
	/**
	 * BEER ME
	 * 
	 * @return
	 * @throws IOException
	 */
	public Beer getNextBeer() throws IOException {
		
		if (false == this.beerJsonElementIterator.hasNext()) {
			return null;
		}
		
		JsonNode beer_review = this.beerJsonElementIterator.next();
		Beer b = this.jacksonJSONObjectMapper.readValue( beer_review.toString(), Beer.class );
		
		//this.jacksonJSONObjectMapper.
		
		this.count++;
		/*
		while (b.text.length() < 1) {
			System.err.println( "Skipped empty beer at count: " + this.count );
			beer_review = this.beerJsonElementIterator.next();
			b = this.jacksonJSONObjectMapper.readValue( beer_review.toString(), Beer.class );
			this.count++;
		}
		*/
		
			
		//	System.out.println("review: " + b.text );
		//	System.out.println("\tuser: " + b.user );
		//	System.out.println("\trating overall: " + b.rating_overall );
			

		return b;
		
	}
	
	public boolean hasNext() {
		
		return this.beerJsonElementIterator.hasNext();
		
	}	
	
	
}
