package io.skymind.experimental.vectorization.csv.spark;

import io.skymind.experimental.vectorization.csv.CSVInputVectorizationSchema;

import org.apache.spark.api.java.function.Function;


/*

        JavaRDD< LabeledPoint > trainingRecords = rawCSVRecords_trainingRecords.map(new Function< String, LabeledPoint >() {
            @Override
            public LabeledPoint call(String rawRecordString) throws Exception {
                //Vector features = v1.features();
                //Vector normalized = scalarModel.transform(features);
                //return new LabeledPoint( v1.label(), v1.features() );
            	
            	String[] parts = rawRecordString.split(",");
            	
            	
            	//String custID = SVMLightUtils.getUniqueIDFromAeolipileRecord( rawRecordString );
            	//String svmLight = SVMLightUtils.getSVMLightRecordFromAeolipileRecord( rawRecordString );
            	String label = parts[ 0 ]; //SVMLightUtils.getLabel( svmLight );
            	double dLabel = Double.parseDouble( label );
            	
            	//Vector svmLightVector = convertSVMLightToVector(svmLight, parsedSVMLightFeatureColumns);
            	Vector csvVector = Utils.convert_CSV_To_Dense_Vector( rawRecordString, parsedSVMLightFeatureColumns );
            	
            	
            
                //return new Tuple2<String, String>( custID, (max + "") );
            	return new LabeledPoint( dLabel, csvVector );
            }
        }).cache();  

*/

public class CSVVectorizeFunction implements Function< String, String > {

	CSVInputVectorizationSchema schema = null;
	
	public CSVVectorizeFunction(CSVInputVectorizationSchema s) {
		
		this.schema = s;
		
	}	
	
	
	@Override
	public String call(String csvLine) throws Exception {
		
		return this.schema.vectorizePlainCSV( csvLine );
		
	}

}
