package io.skymind.hdp.cert.spark.dl4j.mlp;

import static org.junit.Assert.*;

import org.apache.spark.mllib.linalg.DenseVector;
import org.apache.spark.mllib.linalg.Vector;
import org.junit.Test;

//import com.pattersonconsultingtn.ranger.aeolipile.utils.SVMLightUtils;

public class TestVectorParsing {

	@Test
	public void testDenseVectorParse() {
/*	
		String rawCSV = "0,-0.976997367730473,-0.594055002855628";
		int featureColCount = 2;
		
		//String svmlight = SVMLightUtils.getSVMLightRecordFromAeolipileRecord( aeolipileRecord );
		//String label = SVMLightUtils.getLabel(svmlight);
		
		//DenseVector v = SVMLightUtils.convertSVMLightTo_Dense_Vector(svmlight, 16);
		
		DenseVector v = MLP_Saturn_SparkJob.convert_CSV_To_Dense_Vector( rawCSV, featureColCount );
		
		System.out.println( v.toString() + "\n\n" );
		
		System.out.println( v.toArray().length );
		
		assertEquals( featureColCount, v.toArray().length );
		//assertEquals( "0.0", label );
		assertEquals( -0.976997367730473, v.toArray()[0], 0.0 );
		assertEquals( -0.594055002855628, v.toArray()[1], 0.0 );
		//assertEquals( 0.0, v.toArray()[7], 0.0 );
		//assertEquals( 0.3333333333333333, v.toArray()[10], 0.0001 );
		//assertEquals( 0.25, v.toArray()[15], 0.0 );
*/		
		
		int c = 0;
		for (int x = 0; x < 100; x++ ) {
			
			System.out.println( c );
			
			c++;
			
			
		}
		
		
	}

}
