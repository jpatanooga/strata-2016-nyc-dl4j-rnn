package org.deeplearning4j.examples.rnn.beer.utils;

import static org.junit.Assert.*;

import java.util.ArrayList;

import org.deeplearning4j.examples.utils.Utils;
import org.junit.Test;

public class TestUtils {

	@Test
	public void test() {
		//fail("Not yet implemented");
		
		ArrayList<String> lines = new ArrayList<>();
		lines.add("alpha");
		lines.add("beta");
		lines.add("gamma");
		
		Utils.writeLinesToLocalDisk("/tmp/rnn_util.txt", lines);
		
	}

}
