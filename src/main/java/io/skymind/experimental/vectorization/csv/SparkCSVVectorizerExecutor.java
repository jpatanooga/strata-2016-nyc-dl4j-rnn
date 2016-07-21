package io.skymind.experimental.vectorization.csv;

//import io.skymind.experimental.vectorization.csv.spark.SparkCSVStatsMapFunction;





import io.skymind.experimental.vectorization.csv.spark.CSVCollectStats_CollectCount;
import io.skymind.experimental.vectorization.csv.spark.CSVCollectStats_Combine;
import io.skymind.experimental.vectorization.csv.spark.CSVDeriveStats_CollectCount;
import io.skymind.experimental.vectorization.csv.spark.CSVDeriveStats_Combine;
import io.skymind.experimental.vectorization.csv.spark.CSVVectorizeFunction;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.Accumulator;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.datavec.api.writable.Writable;
import org.datavec.spark.transform.misc.WritablesToStringFunction;
import org.nd4j.linalg.dataset.DataSet;

import scala.Tuple2;




/**
 * Based loosely on the ideas in: SparkTransformExecutor
 * 
 * 
 * Example:
 * 
 * JavaRDD<List<Writable>> vectorizedData = exec.execute(inputCSV, schema);
 * 
 * 
 * @author josh
 *
 */
public class SparkCSVVectorizerExecutor {
	
	CSVInputVectorizationSchema schema = null;
	
	public JavaRDD<String> execute(JavaSparkContext sc, CSVInputVectorizationSchema schema, JavaRDD<String> csvData) {
		
	//	JavaRDD<String> processedAsString = processedData.map(new WritablesToStringFunction(","));
		
		this.schema = schema;
		
		// Accumulator<Vector> vecAccum = sc.accumulator(new Vector(...), new VectorAccumulatorParam());
		
		//Accumulator<CSVInputVectorizationSchema> schemaAccum = sc.accumulator( schema, new SparkSchemaAccumulator() );
		
		
		
		
		
/*		
	
        for(String s : iris_sample) {
        	//System.out.println(s);
        	
        	schema.evaluateInputRecord(s);
        }
        
        // TODO: collect all of the schemas, merge the stats
        
        schema.computeDatasetStatistics();

        for(String s : iris_sample) {
        	//System.out.println(s);
        	
        	schema.evaluateInputRecordDerivedStatistics(s);
        }
        
        // TODO: collect all of the schemas, merge the stats        
        
        schema.computeDatasetDerivedStatistics();
        
        
        schema.debugPrintDatasetStatistics();
        
        List<String> transformedVectors = new ArrayList<>();

        for(String s : iris_sample) {
        	//System.out.println(s);
        	transformedVectors.add(schema.vectorizeSVMLightCSVFormat( s ));
        	//System.out.println( schema.vectorizeSVMLightCSVFormat( s ) );
        }
        
        
        JavaRDD<String> transformedVectorsRDD = sc.parallelize( transformedVectors );// sc.textFile(directory);
        	
		
*/		
		
		
		
		
		
		// 1.) MAP the input CSV data and collect stats
		
		
		//			do we treat copies of the schema as a broadcast variable?
		//			how do we collect the schemas?
		//			how would we rectify them
		
		
		// ALTERNATIVE PATH:
		// 1. Scan dataset for min per column
		// 2. scan dataset for max per column
		// 3. scan dataset for sum per column
		
		
		/*
		// CURRENT PATH
		// this function splits the columns into {K,V} pairs of { colName, vale }
		CSVCollectStatsFlatMapFunction csvStatsPassFunc = new CSVCollectStatsFlatMapFunction( schema );
		
        //JavaRDD<Tuple2<String, String>> collectedStatsFlatMapResults = csvData.flatMap( csvStatsPassFunc );
        JavaRDD<Tuple2<String, String>> collectedStatsFlatMapResults = csvData.flatMap( csvStatsPassFunc );
        //collectedStatsFlatMapResults.mapToPair(f)
        
        JavaPairRDD< String, String > collectedStatsFlatMapResultsPairRDD = JavaPairRDD.fromJavaRDD( collectedStatsFlatMapResults );
        
        JavaPairRDD<String, Iterable<String>> groupedColumnsJavaPairRDD = collectedStatsFlatMapResultsPairRDD.groupByKey();
 
        //groupedColumnsJavaPairRDD.re
          
         
         
        
        List< Tuple2<String, String> > localList = collectedStatsFlatMapResults.collect();
        
        // now collect/filter/reduce
        
        for(Tuple2<String, String> s : localList) {
        	System.out.println(s);
        }
        */
		
		// setup initial map
//		Map<String, CSVSchemaColumn> columnSchemasInitial = new LinkedHashMap<>();
	//	schema.getColumnSchemas().putAll(columnSchemasInitial);
		
		CSVCollectStats_CollectCount collectFunc = new CSVCollectStats_CollectCount( schema );
		CSVCollectStats_Combine combineFunc = new CSVCollectStats_Combine( schema );
		Map<String, CSVSchemaColumn> columnSchemasResult = csvData.aggregate( schema.getColumnSchemas(), collectFunc, combineFunc);
		
		schema.setColumnSchemas(columnSchemasResult);
		schema.computeDatasetStatistics();

		System.out.println( "Running Second Stats Pass ------- " );
		
		// pass two
		CSVDeriveStats_CollectCount derviedCollectFunc = new CSVDeriveStats_CollectCount( schema );
		CSVDeriveStats_Combine derivedCombineFunc = new CSVDeriveStats_Combine( schema );
		Map<String, CSVSchemaColumn> dervivedStatsColumnSchemasResult = csvData.aggregate( schema.getColumnSchemas(), derviedCollectFunc, derivedCombineFunc);
		
		schema.setColumnSchemas( dervivedStatsColumnSchemasResult );
		schema.computeDatasetDerivedStatistics();
		
		
		
		
		
		
		System.out.println( " --- Finished Stats --- " ); 
		schema.debugPrintDatasetStatistics();
		
//        CSVInputVectorizationSchema mergedSchema = schemaAccum.localValue();
        		
		
		
		// 2.) MAP the input data
		
		CSVVectorizeFunction vectorizeCSV = new CSVVectorizeFunction( schema );
		
		JavaRDD< String > output = csvData.map( vectorizeCSV );
		
		
		return output;
	}
	
	/*
	public static CSVInputVectorizationSchema mergeSchemas(List<CSVInputVectorizationSchema> schemaList) {
		
		CSVInputVectorizationSchema mergedSchema = new CSVInputVectorizationSchema();
		
		for ( CSVInputVectorizationSchema partial: schemaList ) {
			
			mergedSchema.collectPartialStatistics( partial );
			
		}
		
		
		
		return mergedSchema;
		
	}
	*/

}
