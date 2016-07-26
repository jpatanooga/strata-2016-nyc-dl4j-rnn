package org.deeplearning4j.examples.rnn.json;

import static org.junit.Assert.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.deeplearning4j.examples.rnn.beer.schema.json.Beer;
import org.junit.Test;
//import org.json.*;



import com.fasterxml.jackson.databind.JsonNode;






import com.fasterxml.jackson.databind.ObjectMapper;

public class TestJSONReader {

	@Test
	public void test() throws IOException {
		//fail("Not yet implemented");
		
		String pathToTestData = "/Users/josh/Documents/Talks/2016/Strata_NYC/data/beer/beers_all.json";
		
		//org.json.
		
		Map<String, Beer> beerMap = new HashMap<String, Beer>();
		
		byte[] jsonData = Files.readAllBytes(Paths.get( pathToTestData ));
		
		// ObjectMapper mapper = new ObjectMapper();
		
		Map<String,String> myMap = new HashMap<String, String>();
		
		ObjectMapper objectMapper = new ObjectMapper();
		//objectMapper.
		//myMap = objectMapper.readValue(jsonData, HashMap.class);
		//System.out.println("Map is: "+myMap);	
		
		com.fasterxml.jackson.databind.JsonNode rootNode = objectMapper.readTree(jsonData);
		
		Iterator<JsonNode> root_iter = rootNode.elements();
		
		JsonNode list = root_iter.next();
		
		Iterator<JsonNode> elements = list.elements();
		
		for ( int x = 0; x < 10; x++ ) {
			
			JsonNode beer = elements.next();
			System.out.println("beer: " + beer.toString());
			System.out.println( "\tID: " + beer.get("id") );
			//beer.
			
			Beer b = objectMapper.readValue( beer.toString(), Beer.class );
			
			beerMap.put( beer.get("id").toString(), b );
		}
		
	}

}
