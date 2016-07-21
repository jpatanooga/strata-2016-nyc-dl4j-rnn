package io.skymind.experimental.vectorization.csv.spark;

import io.skymind.experimental.vectorization.csv.CSVInputVectorizationSchema;
import io.skymind.experimental.vectorization.csv.CSVSchemaColumn;

import java.util.Map;

import org.apache.spark.api.java.function.Function2;

public class CSVDeriveStats_CollectCount implements Function2<Map<String, CSVSchemaColumn> , String, Map<String, CSVSchemaColumn> > {
	
	CSVInputVectorizationSchema schema = null;
	
	public CSVDeriveStats_CollectCount(CSVInputVectorizationSchema s) {
		
		this.schema = s;
		
	}
	
	/**
	 * ISSUE: we rolling up the partial sum here over and over
	 * 
	 * 
	 */
	@Override
	public Map<String, CSVSchemaColumn> call(Map<String, CSVSchemaColumn> columnSchemas, String csvLine) throws Exception {
		
		this.schema.evaluateInputRecordDerivedSpark( columnSchemas, csvLine );
		
		return columnSchemas;
	}
}
