package io.skymind.experimental.vectorization.csv.spark;

import java.util.Map;

import io.skymind.experimental.vectorization.csv.CSVInputVectorizationSchema;
import io.skymind.experimental.vectorization.csv.CSVSchemaColumn;

import org.apache.spark.api.java.function.Function2;

/**
 * Based on:
 * 
 * https://github.com/databricks/learning-spark/blob/master/src/main/java/com/oreilly/learningsparkexamples/java/BasicAvg.java
 * 
 * 
 * 
 * @author josh
 *
 */
public class CSVCollectStats_CollectCount implements Function2<Map<String, CSVSchemaColumn> , String, Map<String, CSVSchemaColumn> > {

	
	CSVInputVectorizationSchema schema = null;
	
	public CSVCollectStats_CollectCount(CSVInputVectorizationSchema s) {
		
		this.schema = s;
		
		
		
	}
	
	/**
	 * ISSUE: we rolling up the partial sum here over and over
	 * 
	 * 
	 */
	@Override
	public Map<String, CSVSchemaColumn> call(Map<String, CSVSchemaColumn> columnSchemas, String csvLine) throws Exception {
		// TODO Auto-generated method stub
		
		//this.schema.evaluateInputRecord(csvLine);
		this.schema.evaluateInputRecordSpark( columnSchemas, csvLine );
		
	//	return this.schema.compareColumnsForMinimum(csvLine0, csvLine1);
		//return this.schema.getColumnSchemas();
		return columnSchemas;
	}
}
