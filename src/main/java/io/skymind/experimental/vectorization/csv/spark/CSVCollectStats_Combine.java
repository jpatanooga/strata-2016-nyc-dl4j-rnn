package io.skymind.experimental.vectorization.csv.spark;

import io.skymind.experimental.vectorization.csv.CSVInputVectorizationSchema;
import io.skymind.experimental.vectorization.csv.CSVSchemaColumn;

import java.util.Map;

import org.apache.spark.api.java.function.Function2;

public class CSVCollectStats_Combine implements Function2< Map<String, CSVSchemaColumn> , Map<String, CSVSchemaColumn>, Map<String, CSVSchemaColumn> > {


	CSVInputVectorizationSchema schema = null;
	
	public CSVCollectStats_Combine(CSVInputVectorizationSchema s) {
		
		this.schema = s;
		
		
		
	}	
	
	@Override
	public Map<String, CSVSchemaColumn> call(Map<String, CSVSchemaColumn> columnSchemas, Map<String, CSVSchemaColumn> columnSchemasOther) throws Exception {
		// TODO Auto-generated method stub
		
		this.schema.mergeColumnStatistics(columnSchemas, columnSchemasOther);
		
		
		return columnSchemas;
	}

}
