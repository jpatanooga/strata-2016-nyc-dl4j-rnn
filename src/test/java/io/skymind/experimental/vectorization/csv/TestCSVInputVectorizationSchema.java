package io.skymind.experimental.vectorization.csv;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.datavec.api.records.reader.RecordReader;
import org.datavec.api.records.reader.impl.csv.CSVRecordReader;
import org.datavec.api.writable.Writable;
import org.datavec.spark.transform.misc.StringToWritablesFunction;
import org.junit.Test;

//import com.pattersonconsultingtn.ranger.phalanx.vectorization.csv.schema.CSVInputSchema;

public class TestCSVInputVectorizationSchema {

	
	@Test
	public void testMergeSchema() throws Exception {

		CSVInputVectorizationSchema schemaMaster = new CSVInputVectorizationSchema();
		schemaMaster.parseSchemaFile("src/test/resources/vectorization/csv/basic_iris_vectorization_schema.txt");
		
        List<String> iris_sample = Arrays.asList(		
"5.1,3.5,1.4,0.2,Iris-setosa",
"4.9,3.0,1.4,0.2,Iris-setosa",
"4.7,3.2,1.3,0.2,Iris-setosa",
"7.0,3.2,4.7,1.4,Iris-versicolor",
"6.4,3.2,4.5,1.5,Iris-versicolor",
"6.9,3.1,4.9,1.5,Iris-versicolor",
"5.5,2.3,4.0,1.3,Iris-versicolor",
"6.5,2.8,4.6,1.5,Iris-versicolor",
"6.3,3.3,6.0,2.5,Iris-virginica",
"5.8,2.7,5.1,1.9,Iris-virginica",
"7.1,3.0,5.9,2.1,Iris-virginica",
"6.3,2.9,5.6,1.8,Iris-virginica"
);
		
        
        for(String s : iris_sample) {
        	//System.out.println(s);
        	
        	schemaMaster.evaluateInputRecord(s);
        }
        
        schemaMaster.computeDatasetStatistics();		
        
        schemaMaster.debugPrintDatasetStatistics();
		
        
        // --------------------------------------------
        
		CSVInputVectorizationSchema schemaPartial_0 = new CSVInputVectorizationSchema();
		schemaPartial_0.parseSchemaFile("src/test/resources/vectorization/csv/basic_iris_vectorization_schema.txt");
		
        List<String> iris_sample_split_0 = Arrays.asList(		
"5.1,3.5,1.4,0.2,Iris-setosa",
"4.9,3.0,1.4,0.2,Iris-setosa",
"4.7,3.2,1.3,0.2,Iris-setosa",
"7.0,3.2,4.7,1.4,Iris-versicolor",
"6.4,3.2,4.5,1.5,Iris-versicolor",
"6.9,3.1,4.9,1.5,Iris-versicolor"

);        
        
        for(String s_0 : iris_sample_split_0) {
        	
        	schemaPartial_0.evaluateInputRecord( s_0 );
        }
        
        schemaPartial_0.debugPrintDatasetStatistics();
        
        

		CSVInputVectorizationSchema schemaPartial_1 = new CSVInputVectorizationSchema();
		schemaPartial_1.parseSchemaFile("src/test/resources/vectorization/csv/basic_iris_vectorization_schema.txt");
        
        
        List<String> iris_sample_split_1 = Arrays.asList(		

        "5.5,2.3,4.0,1.3,Iris-versicolor",
        "6.5,2.8,4.6,1.5,Iris-versicolor",
        "6.3,3.3,6.0,2.5,Iris-virginica",
        "5.8,2.7,5.1,1.9,Iris-virginica",
        "7.1,3.0,5.9,2.1,Iris-virginica",
        "6.3,2.9,5.6,1.8,Iris-virginica"
       );
        
        for(String s_1 : iris_sample_split_1) {
        	
        	schemaPartial_1.evaluateInputRecord( s_1 );
        }
        
        
        schemaPartial_1.debugPrintDatasetStatistics();
        
        schemaPartial_0.collectPartialStatistics(schemaPartial_1);
        
        // now check counts
        
        assertEquals( schemaMaster.getColumnSchemaByName("sepallength").minValue, 4.7, 0.00001 );
        assertEquals( schemaMaster.getColumnSchemaByName("sepallength").maxValue, 7.1, 0.00001 );
        assertEquals( schemaMaster.getColumnSchemaByName("sepallength").sum, 72.49999999999999, 0.00001 );

        assertEquals( schemaMaster.getColumnSchemaByName("sepallength").minValue, schemaPartial_0.getColumnSchemaByName("sepallength").minValue, 0.00001 );
        assertEquals( schemaMaster.getColumnSchemaByName("sepallength").maxValue, schemaPartial_0.getColumnSchemaByName("sepallength").maxValue, 0.00001 );
        assertEquals( schemaMaster.getColumnSchemaByName("sepallength").sum, schemaPartial_0.getColumnSchemaByName("sepallength").sum, 0.00001 );
        
        
        assertEquals( schemaMaster.getColumnSchemaByName("class").recordLabels.size(), schemaPartial_0.getColumnSchemaByName("class").recordLabels.size() );
        
        assertEquals( 2, schemaPartial_1.getColumnSchemaByName("class").recordLabels.size() );
        
	}
	
	
	
	
	@Test
	public void test() throws Exception {
		//fail("Not yet implemented");
		
		CSVInputVectorizationSchema schema = new CSVInputVectorizationSchema();
		schema.parseSchemaFile("src/test/resources/vectorization/csv/basic_iris_vectorization_schema.txt");

		//System.out.println("Schema: \n" + schema.rawTextSchema );
		
		
		

		
/*		
        List<String> data_tmp = Arrays.asList( //"sup1", "sup2", "sup3");
        		"2016-01-01 17:00:00.000,830a7u3,u323fy8902,1,USA,100.00,Legit",
        		"2016-01-01 18:03:01.256,830a7u3,9732498oeu,3,FR,73.20,Legit",
        		"2016-01-03 02:53:32.231,78ueoau32,w234e989,1,USA,1621.00,Fraud",
        		"2016-01-03 09:30:16.832,t842uocd,9732498oeu,4,USA,43.19,Legit",
        		"2016-01-04 23:01:52.920,t842uocd,cza8873bm,10,MX,159.65,Legit",
        		"2016-01-05 02:28:10.648,t842uocd,fgcq9803,6,CAN,26.33,Fraud",
        		"2016-01-05 10:15:36.483,rgc707ke3,tn342v7,2,USA,-0.90,Legit" );
*/
		
        List<String> iris_sample = Arrays.asList(		
"5.1,3.5,1.4,0.2,Iris-setosa",
"4.9,3.0,1.4,0.2,Iris-setosa",
"4.7,3.2,1.3,0.2,Iris-setosa",
"7.0,3.2,4.7,1.4,Iris-versicolor",
"6.4,3.2,4.5,1.5,Iris-versicolor",
"6.9,3.1,4.9,1.5,Iris-versicolor",
"5.5,2.3,4.0,1.3,Iris-versicolor",
"6.5,2.8,4.6,1.5,Iris-versicolor",
"6.3,3.3,6.0,2.5,Iris-virginica",
"5.8,2.7,5.1,1.9,Iris-virginica",
"7.1,3.0,5.9,2.1,Iris-virginica",
"6.3,2.9,5.6,1.8,Iris-virginica"
);
		
        
        for(String s : iris_sample) {
        	//System.out.println(s);
        	
        	schema.evaluateInputRecord(s);
        }
        
        schema.computeDatasetStatistics();

        for(String s : iris_sample) {
        	//System.out.println(s);
        	
        	schema.evaluateInputRecordDerivedStatistics(s);
        }
        
        schema.computeDatasetDerivedStatistics();
        
        
        schema.debugPrintDatasetStatistics();

        for(String s : iris_sample) {
        	//System.out.println(s);
        	
        	System.out.println( schema.vectorizeSVMLightCSVFormat( s ) );
        }
        
        
        
        // TODO: 1.) get from String(CSV line) to RDD<DataSet> ---- how?

/*
 * 
 * 		// this snippet should get us to List<Writable> in JavaRDD form
 * 
        JavaRDD<String> stringData = sc.parallelize( data_tmp );// sc.textFile(directory);
       

        //We first need to parse this format. It's comma-delimited (CSV) format, so let's parse it using CSVRecordReader:
        RecordReader rr = new CSVRecordReader();
        JavaRDD<List<Writable>> parsedInputData = stringData.map(new StringToWritablesFunction(rr));

 *         
 */
        
        
        // TODO: 2.) make jump from List<Writable> to JavaRDD<DataSet>
        // -------------> CanovaDataSetFunction
        
/*        
   
		JavaRDD<Collection<Writable>> rdd = origData.map(rrf);
        JavaRDD<DataSet> data = rdd.map(new CanovaDataSetFunction(28*28,2,false));   
   
        */
        
		
	}

}
