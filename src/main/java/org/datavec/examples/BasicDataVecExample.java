package org.datavec.examples;

import io.skymind.experimental.vectorization.csv.CSVInputVectorizationSchema;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;



//import org.canova.api.records.reader.RecordReader;
//import org.canova.api.records.reader.SequenceRecordReader;

//import org.canova.api.records.reader.impl.CSVRecordReader;

//import org.canova.api.records.reader.impl.CSVSequenceRecordReader;
import org.nd4j.linalg.dataset.DataSet;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
//import org.canova.cli.csv.schema.CSVInputSchema;
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
///import org.deeplearning4j.spark.canova.CanovaDataSetFunction;
//import org.deeplearning4j.spark.canova.RecordReaderFunction;

//import org.canova.api.records.reader.RecordReader;

import org.joda.time.DateTimeFieldType;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
//import org.nd4j.linalg.dataset.api.DataSet;

public class BasicDataVecExample {

    private static final Logger log = LoggerFactory.getLogger(BasicDataVecExample.class);
    public static  void main(String[] args) throws Exception {


        //=====================================================================
        //                 Step 1: Define the input data schema
        //=====================================================================

        //Let's define the schema of the data that we want to import
        //The order in which columns are defined here should match the order in which they appear in the input data
        Schema inputDataSchema = new Schema.Builder()
            //We can define a single column
            .addColumnString("DateTimeString")
            //Or for convenience define multiple columns of the same type
            .addColumnsString("CustomerID", "MerchantID")
            //We can define different column types for different types of data:
            .addColumnInteger("NumItemsInTransaction")
            .addColumnCategorical("MerchantCountryCode", Arrays.asList("USA","CAN","FR","MX"))
            //Some columns have restrictions on the allowable values, that we consider valid:
            .addColumnDouble("TransactionAmountUSD",0.0,null,false,false)   //$0.0 or more, no maximum limit, no NaN and no Infinite values
            .addColumnCategorical("FraudLabel", Arrays.asList("Fraud","Legit"))
            .build();

        //Print out the schema:
        System.out.println("Input data schema details:");
        System.out.println(inputDataSchema);

        System.out.println("\n\nOther information obtainable from schema:");
        System.out.println("Number of columns: " + inputDataSchema.numColumns());
        System.out.println("Column names: " + inputDataSchema.getColumnNames());
        System.out.println("Column types: " + inputDataSchema.getColumnTypes());


        //=====================================================================
        //            Step 2: Define the operations we want to do
        //=====================================================================

        //Lets define some operations to execute on the data...
        //We do this by defining a TransformProcess
        //At each step, we identify column by the name we gave them in the input data schema, above

        TransformProcess tp = new TransformProcess.Builder(inputDataSchema)
            //Let's remove some column we don't need
            .removeColumns("CustomerID","MerchantID")

            //Now, suppose we only want to analyze transactions involving merchants in USA or Canada. Let's filter out
            // everthing except for those countries.
            //Here, we are applying a conditional filter. We remove all of the examples that match the condition
            // The condition is "MerchantCountryCode" isn't one of {"USA", "CAN"}
            .filter(new ConditionFilter(
                new CategoricalColumnCondition("MerchantCountryCode", ConditionOp.NotInSet, new HashSet<>(Arrays.asList("USA","CAN")))))

            //Let's suppose our data source isn't perfect, and we have some invalid data: negative dollar amounts that we want to replace with 0.0
            //For positive dollar amounts, we don't want to modify those values
            //Use the ConditionalReplaceValueTransform on the "TransactionAmountUSD" column:
            .conditionalReplaceValueTransform(
                "TransactionAmountUSD",     //Column to operate on
                new DoubleWritable(0.0),    //New value to use, when the condition is satisfied
                new DoubleColumnCondition("TransactionAmountUSD",ConditionOp.LessThan, 0.0)) //Condition: amount < 0.0

            //Finally, let's suppose we want to parse our date/time column in a format like "2016/01/01 17:50.000"
            //We use JodaTime internally, so formats can be specified as follows: http://www.joda.org/joda-time/apidocs/org/joda/time/format/DateTimeFormat.html
            .stringToTimeTransform("DateTimeString","YYYY-MM-DD HH:mm:ss.SSS", DateTimeZone.UTC)

            //However, our time column ("DateTimeString") isn't a String anymore. So let's rename it to something better:
            .renameColumn("DateTimeString", "DateTime")

            //At this point, we have our date/time format stored internally as a long value (Unix/Epoch format): milliseconds since 00:00.000 01/01/1970
            //Suppose we only care about the hour of the day. Let's derive a new column for that, from the DateTime column
            .transform(new DeriveColumnsFromTimeTransform.Builder("DateTime")
                .addIntegerDerivedColumn("HourOfDay", DateTimeFieldType.hourOfDay())
                .build())

            //We no longer need our "DateTime" column, as we've extracted what we need from it. So let's remove it
            .removeColumns("DateTime")

            //We've finished with the sequence of operations we want to do: let's create the final TransformProcess object
            .build();


        //After executing all of these operations, we have a new and different schema:
        Schema outputSchema = tp.getFinalSchema();

        System.out.println("\n\n\nSchema after transforming data:");
        System.out.println(outputSchema);


        //=====================================================================
        //      Step 3: Load our data and execute the operations on Spark
        //=====================================================================

        //We'll use Spark local to handle our data

        SparkConf conf = new SparkConf();
        conf.setMaster("local[*]");
        conf.setAppName("DataVec Example");

        JavaSparkContext sc = new JavaSparkContext(conf);

        List<String> data_tmp = Arrays.asList( //"sup1", "sup2", "sup3");
        		"2016-01-01 17:00:00.000,830a7u3,u323fy8902,1,USA,100.00,Legit",
        		"2016-01-01 18:03:01.256,830a7u3,9732498oeu,3,FR,73.20,Legit",
        		"2016-01-03 02:53:32.231,78ueoau32,w234e989,1,USA,1621.00,Fraud",
        		"2016-01-03 09:30:16.832,t842uocd,9732498oeu,4,USA,43.19,Legit",
        		"2016-01-04 23:01:52.920,t842uocd,cza8873bm,10,MX,159.65,Legit",
        		"2016-01-05 02:28:10.648,t842uocd,fgcq9803,6,CAN,26.33,Fraud",
        		"2016-01-05 10:15:36.483,rgc707ke3,tn342v7,2,USA,-0.90,Legit" );

        
        String directory = "/Users/josh/Documents/workspace/Skymind/Lloyds_Bank/src/main/resources/BasicDataVecExample/exampledata.csv"; 
        		//new ClassPathResource("/Users/josh/Documents/workspace/Skymind/Lloyds_Bank/src/main/resources/BasicDataVecExample/exampledata.csv").getFile().getParent(); //Normally just define your directory like "file:/..." or "hdfs:/..."
        
        //System.out.println( sc.textFile(directory) );
        
        JavaRDD<String> stringData = sc.parallelize( data_tmp );// sc.textFile(directory);
        //sc.

        //We first need to parse this format. It's comma-delimited (CSV) format, so let's parse it using CSVRecordReader:
        RecordReader rr = new CSVRecordReader();
        JavaRDD<List<Writable>> parsedInputData = stringData.map(new StringToWritablesFunction(rr));

        //Now, let's execute the transforms we defined earlier:
        SparkTransformExecutor exec = new SparkTransformExecutor();
        JavaRDD<List<Writable>> processedData = exec.execute(parsedInputData, tp);

        
        
        
        
        
        
        
        //For the sake of this example, let's collect the data locally and print it:
        JavaRDD<String> processedAsString = processedData.map(new WritablesToStringFunction(","));
        //processedAsString.saveAsTextFile("file://your/local/save/path/here");   //To save locally
        //processedAsString.saveAsTextFile("hdfs://your/hdfs/save/path/here");   //To save to hdfs

        
        
        
        
        
        
        List<String> processedCollected = processedAsString.collect();
        List<String> inputDataCollected = stringData.collect();


        System.out.println("\n\n---- Original Data ----");
        for(String s : inputDataCollected) System.out.println(s);

        System.out.println("\n\n---- Processed Data ----");
        for(String s : processedCollected) System.out.println(s);


        System.out.println("\n\nDONE");
        
        /*
        String schemaFilePath = "src/test/resources/csv/schemas/unit_test_schema.txt";
		CSVInputSchema inputSchema = new CSVInputSchema();
		inputSchema.parseSchemaFile( schemaFilePath );
		
		
		inputSchema.debugPrintColumns();
		*/
        
        String schemaText = "@RELATION SytheticDatasetUnitTest\n";
        schemaText += "@DELIMITER ,\n";
         
        schemaText += "   @ATTRIBUTE sepallength  NUMERIC   !COPY\n";
        schemaText += "@ATTRIBUTE sepalwidth   NUMERIC   !SKIP\n";
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
        
        List<String> transformedVectors = new ArrayList<>();

        for(String s : iris_sample) {
        	//System.out.println(s);
        	transformedVectors.add(schema.vectorizeSVMLightCSVFormat( s ));
        	//System.out.println( schema.vectorizeSVMLightCSVFormat( s ) );
        }
        
        /*
        JavaRDD<String> transformedVectorsRDD = sc.parallelize( transformedVectors );// sc.textFile(directory);
        

        //import org.canova.api.records.reader.RecordReader;
        //import org.canova.api.records.reader.impl.CSVRecordReader;        
        
        //We first need to parse this format. It's comma-delimited (CSV) format, so let's parse it using CSVRecordReader:
        org.canova.api.records.reader.RecordReader vectorCSVReader = new org.canova.api.records.reader.impl.CSVRecordReader(0,",");
        
        //JavaRDD<List<Writable>> parsedVectorsRDD = transformedVectorsRDD.map(new StringToWritablesFunction( rr ));
        
        int labelIndex = 4;
        int numOutputClasses = 3;
        JavaRDD<DataSet> irisTrainingDataRDD = transformedVectorsRDD.map(new RecordReaderFunction(vectorCSVReader, labelIndex, numOutputClasses));
        
        int vec_count = (int) irisTrainingDataRDD.count();
        
        System.out.println( "Converted Vectors: " + vec_count );
        
        //parsedVectorsRDD = stringData.map(new StringToWritablesFunction(rr));
        
//        JavaRDD<DataSet> data = parsedVectorsRDD.map( new CanovaDataSetFunction(vectorCSVReader, 0, 3 ) );
        */
        
    }		
	
}
