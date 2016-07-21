package org.deeplearning4j.examples.canova.spark;

import io.skymind.experimental.vectorization.csv.CSVInputVectorizationSchema;
import io.skymind.experimental.vectorization.csv.SparkCSVVectorizerExecutor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.datavec.api.records.reader.RecordReader;
import org.datavec.api.records.reader.impl.csv.CSVRecordReader;
import org.datavec.api.transform.TransformProcess;
import org.datavec.api.transform.condition.ConditionOp;
import org.datavec.api.transform.condition.column.CategoricalColumnCondition;
import org.datavec.api.transform.condition.column.DoubleColumnCondition;
import org.datavec.api.transform.filter.ConditionFilter;
import org.datavec.api.transform.schema.Schema;
import org.datavec.api.transform.transform.time.DeriveColumnsFromTimeTransform;
import org.datavec.api.writable.DoubleWritable;
import org.datavec.api.writable.Writable;
import org.datavec.spark.transform.SparkTransformExecutor;
import org.datavec.spark.transform.misc.StringToWritablesFunction;
import org.datavec.spark.transform.misc.WritablesToStringFunction;
import org.deeplearning4j.spark.canova.RecordReaderFunction;
import org.deeplearning4j.spark.util.MLLibUtil;
import org.joda.time.DateTimeFieldType;
import org.joda.time.DateTimeZone;
import org.nd4j.linalg.api.ndarray.INDArray;
import org.nd4j.linalg.dataset.DataSet;

import scala.Tuple2;

public class ParallelCSVCanovaTestJob {

	

    public static  void main(String[] args) throws Exception {
    	
        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.WARN);

        
        SparkConf conf = new SparkConf();
        conf.setMaster("local[*]");
        conf.setAppName("DataVec Example");

        JavaSparkContext sc = new JavaSparkContext(conf);
        
        
        String schemaText = "@RELATION SytheticDatasetUnitTest\n";
        schemaText += "@DELIMITER ,\n";
         
        schemaText += "   @ATTRIBUTE sepallength  NUMERIC   !COPY\n";
        schemaText += "@ATTRIBUTE sepalwidth   NUMERIC   !ZEROMEAN_ZEROUNITVARIANCE\n";
        schemaText += "@ATTRIBUTE petallength  NUMERIC   !NORMALIZE\n";
        schemaText += "@ATTRIBUTE petalwidth   NUMERIC   !BINARIZE\n";
        schemaText += "@ATTRIBUTE class        NOMINAL   !LABEL\n";
        

		CSVInputVectorizationSchema schema = new CSVInputVectorizationSchema();
		//schema.parseSchemaFile("src/test/resources/vectorization/csv/basic_iris_vectorization_schema.txt");
		
		schema.parseSchemaFromRawText( schemaText );

		
		
		
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
        
        
        
        
        System.out.println( " ------------ [Start] Original Schema ---------------- " );
        
        // TODO: parallelize input
        
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
                
        System.out.println( " ------------ [End] Original Schema ---------------- " );
        
        
        
        
        
		
        // TODO: parallelize input
        /*
        for(String s : iris_sample) {
        	//System.out.println(s);
        	
        	schema.evaluateInputRecord(s);
        }
        */
        
        schema = new CSVInputVectorizationSchema();
        schema.parseSchemaFromRawText( schemaText );
        
        
        JavaRDD<String> irisSampleRDD = sc.parallelize( iris_sample );
        irisSampleRDD = irisSampleRDD.repartition(2);
        
        // irisSampleRDD.
        
        SparkCSVVectorizerExecutor execCSV = new SparkCSVVectorizerExecutor();
        
        System.out.println( "Exec: CSV First Pass" );
        JavaRDD<String> vectorizedDataRDD = execCSV.execute(sc, schema, irisSampleRDD);
        System.out.println( "Exec: CSV First Pass Complete" );
        
        
        List<String> vectorizedCollected = vectorizedDataRDD.collect();
        //List<String> inputDataCollected = stringData.collect();
        
        for(String s : vectorizedCollected) {
        	
        	System.out.println(s);

        }

/*
        System.out.println("\n\n---- Debug CSV Schema Data ----");
        for(String s : processedCollected) System.out.println(s);
        
        */
        
/*
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
        */
        
        //JavaRDD<String> transformedVectorsRDD = sc.parallelize( transformedVectors );// sc.textFile(directory);
        

        //import org.canova.api.records.reader.RecordReader;
        //import org.canova.api.records.reader.impl.CSVRecordReader;        
        
        //We first need to parse this format. It's comma-delimited (CSV) format, so let's parse it using CSVRecordReader:
        org.canova.api.records.reader.RecordReader vectorCSVReader = new org.canova.api.records.reader.impl.CSVRecordReader(0,",");
        //org.canova.api.records.reader.impl.SVMLightRecordReader
        //org.canova.api.records.reader.RecordReader vectorCSVReader_SVMLight = new org.canova.api.records.reader.impl.SVMLightRecordReader();
        
        //vectorCSVReader_SVMLight.
        
        
        //JavaRDD<List<Writable>> parsedVectorsRDD = transformedVectorsRDD.map(new StringToWritablesFunction( rr ));
        
//        org.deeplearning4j.spark.canova.export.
        
        int labelIndex = 4;
        int numOutputClasses = 3;
        JavaRDD<DataSet> irisTrainingDataRDD = vectorizedDataRDD.map(new RecordReaderFunction(vectorCSVReader, labelIndex, numOutputClasses));
        
        int vec_count = (int) irisTrainingDataRDD.count();
        
        System.out.println( "Converted Vectors: " + vec_count );
        
        
        
    }		
		
	
	
}
