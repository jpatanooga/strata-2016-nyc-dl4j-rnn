package io.skymind.experimental.vectorization.csv;

import io.skymind.experimental.vectorization.csv.CSVSchemaColumn.ColumnType;
import io.skymind.experimental.vectorization.csv.CSVSchemaColumn.TransformType;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.math3.util.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple2;

import com.google.common.base.Strings;

public class CSVInputVectorizationSchema implements java.io.Serializable {

	private static final Logger log = LoggerFactory.getLogger(CSVInputVectorizationSchema.class);

	public String relation = "";
	public String delimiter = "";
	private boolean hasComputedStats = false;
	public String rawTextSchema = "";

	// columns: { columnName, column Schema }
	private Map<String, CSVSchemaColumn> columnSchemas = new LinkedHashMap<>();

	public CSVSchemaColumn getColumnSchemaByName( String colName ) {

		return this.columnSchemas.get(colName);

	}

	public Map<String, CSVSchemaColumn> getColumnSchemas() {
		return this.columnSchemas;
	}
	
	public void setColumnSchemas( Map<String, CSVSchemaColumn> columnSchemasNew ) {
		this.columnSchemas = columnSchemasNew;
	}

	private boolean validateRelationLine( String[] lineParts ) {
    return lineParts.length == 2;
  }

	private boolean validateDelimiterLine( String[] lineParts ) {
    return lineParts.length == 2;
  }

	private boolean validateAttributeLine( String[] lineParts ) {
		
		// first check that we have enough parts on the line
		
		if ( lineParts.length != 4 ) {
			return false;
		}
		
		// now check for combinations of { COLUMNTYPE, TRANSFORM } that we dont support
		
		CSVSchemaColumn colValue = this.parseColumnSchemaFromAttribute( lineParts );
		
		
		// 1. Unsupported: { NUMERIC + LABEL }
		
		//if (colValue.columnType == CSVSchemaColumn.ColumnType.NUMERIC && colValue.transform == CSVSchemaColumn.TransformType.LABEL) { 
		//	return false;
		//}
		

		// 2. Unsupported: { NOMINAL + BINARIZE }

		if (colValue.columnType == CSVSchemaColumn.ColumnType.NOMINAL && colValue.transform == CSVSchemaColumn.TransformType.BINARIZE) { 
			return false;
		}


		// 3. Unsupported: { DATE + anything } --- date columns arent finished yet!
		
		if (colValue.columnType == CSVSchemaColumn.ColumnType.DATE ) { 
			return false;
		}
		

		
		return true;
	}

	private boolean validateSchemaLine( String line ) {

		String lineCondensed = line.trim().replace("\t", " ").replaceAll(" +", " ");
		String[] parts = lineCondensed.split(" ");
		
		//System.out.println( "validateSchemaLine: '" + lineCondensed + "' " );

		if ( parts[ 0 ].toLowerCase().equals("@relation") ) {

			return this.validateRelationLine(parts);

		} else if ( parts[ 0 ].toLowerCase().equals("@delimiter") ) {

			return this.validateDelimiterLine(parts);

		} else if ( parts[ 0 ].toLowerCase().equals("@attribute") ) {

			return this.validateAttributeLine(parts);

		} else if ( parts[ 0 ].trim().equals("") ) {

			return true;

		} else {
			
			System.out.println( "Bad attribute: " + parts[ 0 ].toLowerCase() );

			// bad schema line
			log.error("Line attribute matched no known attribute in schema! --- {}", line);
			return false;

		}


		//return true;

	}

	private String parseRelationInformation(String[] parts) {

		return parts[1];

	}

	private String parseDelimiter(String[] parts) {

		return parts[1];

	}

	/**
	 * parse out lines like:
	 * 		@ATTRIBUTE sepallength  NUMERIC   !COPY
	 *
	 * @param parts
	 * @return
	 */
	private CSVSchemaColumn parseColumnSchemaFromAttribute( String[] parts ) {

		String columnName = parts[1].trim();
		String columnType = parts[2].replace("\t", " ").trim();
		String columnTransform = parts[3].trim();
		//System.out.println( "col: '" + columnType.toUpperCase().trim() + "' " );
		CSVSchemaColumn.ColumnType colTypeEnum =
        CSVSchemaColumn.ColumnType.valueOf(columnType.toUpperCase());
		CSVSchemaColumn.TransformType colTransformEnum =
        CSVSchemaColumn.TransformType.valueOf(columnTransform.toUpperCase().substring(1));

		
		
		return new CSVSchemaColumn( columnName, colTypeEnum, colTransformEnum );
	}

	private void addSchemaLine( String line ) {

		// parse out: columnName, columnType, columnTransform
		String lineCondensed = line.replaceAll("\t", " ").trim().replaceAll(" +", " ");
		String[] parts = lineCondensed.split(" ");

		if ( parts[ 0 ].toLowerCase().equals("@relation") ) {

		//	return this.validateRelationLine(parts);
			this.relation = parts[1];

		} else if ( parts[ 0 ].toLowerCase().equals("@delimiter") ) {

		//	return this.validateDelimiterLine(parts);
			this.delimiter = parts[1];

		} else if ( parts[ 0 ].toLowerCase().equals("@attribute") ) {

			String key = parts[1];
			CSVSchemaColumn colValue = this.parseColumnSchemaFromAttribute( parts );

			this.columnSchemas.put( key, colValue );
		}
	}
	
	public void parseSchemaFile(String schemaPath) throws Exception {
		try (BufferedReader br = new BufferedReader(new FileReader(schemaPath))) {
		    for (String line; (line = br.readLine()) != null; ) {
		        // process the line.
		    	System.out.println(line);
				if ( this.isCommentLine(line)) {
					// skip it
				} else {			    	
			    	if (!this.validateSchemaLine(line) ) {
			    		throw new Exception("Bad Schema for CSV Data: \n\t" + line);
			    	}

			    	// now add it to the schema cache
			    	this.addSchemaLine(line);
				}

		    }
		    // line is not visible here.
		}
	}
	
	private boolean isCommentLine(String line) {
		
		String lineTrimmed = line.trim();
		
		if (lineTrimmed.startsWith("#")) {
			return true;
		}
		
		return false;
		
	}

	public void parseSchemaFromRawText(String schemaText) throws Exception {

		//throw new UnsupportedOperationException();
		
//		try  {

			//BufferedReader br = new BufferedReader( new FileReader( schemaPath ) );
			String[] lines = schemaText.split("\n");
			
		    //for (String line; (line = br.readLine()) != null; ) {
			for ( String line : lines ) {
		        // process the line.
				
				//System.out.println( "line: " + line);
				
				if ( this.isCommentLine(line)) {
					
					// skipping comment line
					
				} else {
			    	if (false == this.validateSchemaLine(line) ) {
			    		throw new Exception("Bad Schema for CSV Data");
			    	}
			    	this.rawTextSchema += line + "\n";
			    	// now add it to the schema cache
			    	this.addSchemaLine(line);
				}
		    	
		    }
	}	
	
	public void loadSchemaStatisticsFromDistributedCache(Configuration conf, Path[] localPaths, String basePath) {
		
		// for each column, load stats
		
		for (Map.Entry<String, CSVSchemaColumn> entry : this.columnSchemas.entrySet()) {
			
			entry.getValue().loadPrepPassStatisticsFromDistributedCache(conf, localPaths, basePath);
			
		}
		
		
	}


	public void loadSchemaDerivedStatisticsFromDistributedCache(Configuration conf, Path[] localPaths, String basePath) {
		
		// for each column, load stats
		
		for (Map.Entry<String, CSVSchemaColumn> entry : this.columnSchemas.entrySet()) {
			
			entry.getValue().loadPrepPassDerivedStatisticsFromDistributedCache( conf, localPaths, basePath);
			
		}
		
		
	}
	
	
	/**
	 * Returns how many columns a newly transformed vector should have
	 *
	 *
	 *
	 * @return
	 */
	public int getTransformedVectorSize() {

		int colCount = 0;

		for (Map.Entry<String, CSVSchemaColumn> entry : this.columnSchemas.entrySet()) {
			if (entry.getValue().transform != CSVSchemaColumn.TransformType.SKIP) {
				colCount++;
			}
		}

		return colCount;

	}

	public List< Tuple2<String, String> > mapValuesToColumns(String csvRecordLine) throws Exception {

		// does the record have the same number of columns that our schema expects?

		List< Tuple2<String, String> > ret = new ArrayList<>();
		
		String[] columns = csvRecordLine.split( this.delimiter );

		int colIndex = 0;
		
		for (Map.Entry<String, CSVSchemaColumn> entry : this.columnSchemas.entrySet()) {


			String colKey = entry.getKey();
		    CSVSchemaColumn colSchemaEntry = entry.getValue();

		    // now work with key and value...
		    //colSchemaEntry.evaluateColumnValue( columns[ colIndex ] );
		    ret.add( new Tuple2<>(colKey, columns[ colIndex ]) );

		    colIndex++;

		}		
		
		return ret;
		
	}
	
	
	
	/**
	 * The major question to ask is:
	 * 
	 * - do we continue on this path and treat every column equally?
	 * 		-	con: we end up doing ops on column types that dont need them (e.g. "min on label column")
	 * 
	 * - do we take groups and split into distinct RDDs?
	 * 		-	con: harder
	 * 
	 * 
	 * @param csvRecordLine0
	 * @param csvRecordLine1
	 * @return
	 * @throws Exception
	 */
	public String compareColumnsForMinimum(String csvRecordLine0, String csvRecordLine1) throws Exception {

		// does the record have the same number of columns that our schema expects?

		//List< Tuple2<String, String> > ret = new ArrayList<>();
		
		String[] columns0 = csvRecordLine0.split( this.delimiter );
		String[] columns1 = csvRecordLine1.split( this.delimiter );
		
		String outputCSVLine = "";

		int colIndex = 0;
		
		String minVal = "";
		
		
		for (Map.Entry<String, CSVSchemaColumn> entry : this.columnSchemas.entrySet()) {


			String colKey = entry.getKey();
		    CSVSchemaColumn colSchemaEntry = entry.getValue();

		    // now work with key and value...
		    //colSchemaEntry.evaluateColumnValue( columns[ colIndex ] );
		    //ret.add( new Tuple2<>(colKey, columns[ colIndex ]) );
		    
		    if ( ColumnType.NUMERIC == colSchemaEntry.columnType  ) {
	
				double tmpVal_0 = Double.parseDouble( columns0[ colIndex ] );
				double tmpVal_1 = Double.parseDouble( columns1[ colIndex ] );
				
				// System.out.println( "converted: " + tmpVal );
				
//				if (Double.isNaN(tmpVal)) {
	//				throw new Exception("The column was defined as Numeric yet could not be parsed as a Double");
		//		}
				
				

				if ( tmpVal_0 < tmpVal_1 ) {
					
					minVal = columns0[ colIndex ];
					
				} else {
					
					minVal = columns1[ colIndex ];
					
				}
				
//		    	if ( columns0[ colIndex ]
		    	
		    } else {
		    	
		    	// treat it as a label
		    	minVal = "-";
		    	
		    	
		    }
		    
		    
		    		
		    
		    
		    outputCSVLine += "";
		    if (colIndex < columns0.length - 1) {
		    	outputCSVLine += ",";
		    }

		    colIndex++;

		}		
		
		return outputCSVLine;
		
	}	
	
	public void evaluateInputRecord(String csvRecordLine) throws Exception {

		// does the record have the same number of columns that our schema expects?

		String[] columns = csvRecordLine.split( this.delimiter );

		if (Strings.isNullOrEmpty(columns[0])) {
			System.out.println("Skipping blank line");
			return;
		}

		if (columns.length != this.columnSchemas.size() ) {

			throw new Exception("Row column count does not match schema column count. (" + columns.length + " != " + this.columnSchemas.size() + ") ");

		}

		int colIndex = 0;

		for (Map.Entry<String, CSVSchemaColumn> entry : this.columnSchemas.entrySet()) {


			String colKey = entry.getKey();
		    CSVSchemaColumn colSchemaEntry = entry.getValue();

		    // now work with key and value...
		    colSchemaEntry.evaluateColumnValue( columns[ colIndex ] );

		    colIndex++;

		}

	}
	
	/**
	 * Uses the local schema for complete schema data
	 * 
	 * -- uses the extra col-schema set to track data
	 * 
	 * @param columnSchemas
	 * @param csvRecordLine
	 * @throws Exception
	 */
	public void evaluateInputRecordSpark(Map<String, CSVSchemaColumn> columnSchemas, String csvRecordLine) throws Exception {

		// does the record have the same number of columns that our schema expects?

		String[] columns = csvRecordLine.split( this.delimiter );

		if (Strings.isNullOrEmpty(columns[0])) {
			System.out.println("Skipping blank line");
			return;
		}

		if (columns.length != this.columnSchemas.size() ) {

			throw new Exception("Row column count does not match schema column count. (" + columns.length + " != " + this.columnSchemas.size() + ") ");

		}

		int colIndex = 0;

		for (Map.Entry<String, CSVSchemaColumn> entry : this.columnSchemas.entrySet()) {


			String colKey = entry.getKey();
		    CSVSchemaColumn colSchemaEntry = columnSchemas.get(colKey);

		    // now work with key and value...
		    colSchemaEntry.evaluateColumnValue( columns[ colIndex ] );

		    colIndex++;

		}

	}	

	
	
	/**
	 * Uses the local schema for complete schema data
	 * 
	 * -- uses the extra col-schema set to track data
	 * 
	 * @param columnSchemas
	 * @param csvRecordLine
	 * @throws Exception
	 */
	public void evaluateInputRecordDerivedSpark(Map<String, CSVSchemaColumn> columnSchemas, String csvRecordLine) throws Exception {

		// does the record have the same number of columns that our schema expects?

		String[] columns = csvRecordLine.split( this.delimiter );

		if (Strings.isNullOrEmpty(columns[0])) {
			System.out.println("Skipping blank line");
			return;
		}

		if (columns.length != this.columnSchemas.size() ) {

			throw new Exception("Row column count does not match schema column count. (" + columns.length + " != " + this.columnSchemas.size() + ") ");

		}

		int colIndex = 0;

		for (Map.Entry<String, CSVSchemaColumn> entry : this.columnSchemas.entrySet()) {


			String colKey = entry.getKey();
		    CSVSchemaColumn colSchemaEntry = columnSchemas.get(colKey);

		    // now work with key and value...
		    colSchemaEntry.evaluateColumnValueDerivedStatistics( columns[ colIndex ] );

		    colIndex++;

		}

	}		
	
	public void evaluateInputRecordDerivedStatistics(String csvRecordLine) throws Exception {

		// does the record have the same number of columns that our schema expects?

		String[] columns = csvRecordLine.split( this.delimiter );

		if (Strings.isNullOrEmpty(columns[0])) {
			System.out.println("Skipping blank line");
			return;
		}

		if (columns.length != this.columnSchemas.size() ) {

			throw new Exception("Row column count does not match schema column count. (" + columns.length + " != " + this.columnSchemas.size() + ") ");

		}

		int colIndex = 0;

		for (Map.Entry<String, CSVSchemaColumn> entry : this.columnSchemas.entrySet()) {


			String colKey = entry.getKey();
		    CSVSchemaColumn colSchemaEntry = entry.getValue();

		    // now work with key and value...
		    colSchemaEntry.evaluateColumnValueDerivedStatistics( columns[ colIndex ] );

		    colIndex++;

		}

	}	


	/**
	 * We call this method once we've scanned the entire dataset once to gather column stats
	 *
	 */
	public void computeDatasetStatistics() {

		for (Map.Entry<String, CSVSchemaColumn> entry : this.columnSchemas.entrySet()) {

			String key = entry.getKey();
			CSVSchemaColumn value = entry.getValue();

			value.computeStatistics();
			
		}
		
		this.hasComputedStats = true;
	}
	
	public void computeDatasetDerivedStatistics() {

		for (Map.Entry<String, CSVSchemaColumn> entry : this.columnSchemas.entrySet()) {

			String key = entry.getKey();
			CSVSchemaColumn value = entry.getValue();

			value.computeDerivedStatistics();
			
		}
		
		// signal weve completed derived?
	}
	
	public void collectPartialStatistics( CSVInputVectorizationSchema partial ) {
		
		for (Map.Entry<String, CSVSchemaColumn> entry : this.columnSchemas.entrySet()) {

			String key = entry.getKey();
			CSVSchemaColumn column = entry.getValue();
			
			CSVSchemaColumn columnPartial = partial.columnSchemas.get(key);
			
			column.mergePartialCollectStats(columnPartial);
			
			

		}
		
		
		
		
	}
	

	/**
	 * This is not static because we want to use the local copy of the schema
	 * 
	 * Merges all of the k->v pairs from B into A (updates counts as well)
	 * 
	 * @param columnSchemasA
	 * @param columnSchemasB
	 */
	public void mergeColumnStatistics( Map<String, CSVSchemaColumn> columnSchemasA, Map<String, CSVSchemaColumn> columnSchemasB ) {
		
		for (Map.Entry<String, CSVSchemaColumn> entry : this.columnSchemas.entrySet()) {

			String key = entry.getKey();
			//CSVSchemaColumn column = entry.getValue();
			
			CSVSchemaColumn columnA = columnSchemasA.get(key);
			CSVSchemaColumn columnB = columnSchemasB.get(key);
			
			columnA.mergePartialCollectStats(columnB);
			
			

		}
		
		
		
		
	}	
	
	public void mergeDerivedColumnStatistics( Map<String, CSVSchemaColumn> columnSchemasA, Map<String, CSVSchemaColumn> columnSchemasB ) {
		
		for (Map.Entry<String, CSVSchemaColumn> entry : this.columnSchemas.entrySet()) {

			String key = entry.getKey();
			//CSVSchemaColumn column = entry.getValue();
			
			CSVSchemaColumn columnA = columnSchemasA.get(key);
			CSVSchemaColumn columnB = columnSchemasB.get(key);
			
			columnA.mergePartialDerivedStats(columnB);
			
			

		}
		
		
		
		
	}		
	

	public void debugPrintDatasetStatistics() {

		System.out.println("Print Schema --------");

		for (Map.Entry<String, CSVSchemaColumn> entry : this.columnSchemas.entrySet()) {

			String key = entry.getKey();
			CSVSchemaColumn value = entry.getValue();

		  // now work with key and value...

		  System.out.println("> " + value.name + ", " + value.columnType + ", " + value.transform);

		  if ( value.transform == TransformType.LABEL ) {

			  System.out.println("\t> Label > Class Balance Report ");
			  
			  int totalLabels = 0;

			  //for (Map.Entry<String, Pair<Integer,Integer>> label : value.recordLabels.entrySet()) {
			  for (Map.Entry<String, Tuple2<Integer,Integer>> label : value.recordLabels.entrySet()) {

			  	// value.recordLabels.size()
			  	//System.out.println("\t\t " + label.getKey() + ": " + label.getValue().getFirst() + ", " + label.getValue().getSecond());
				  System.out.println("\t\t " + label.getKey() + ": " + label.getValue()._1() + ", " + label.getValue()._2());

			  	//totalLabels += label.getValue().getSecond();
				  totalLabels += label.getValue()._2();
			  	
			  }
			  
			  System.out.println("\t\tTotal Labels: " + totalLabels);
			  
			  System.out.println("\t\tMissed Label Lookups: " + value.missedLabelLookups);
			  
			  System.out.println("\t\tTotal Values Seen: " + value.count);

		  } else if ( value.columnType == ColumnType.NOMINAL ) {
			  
			  System.out.println("\t> Nominal > Category Balance Report ");
			  
			  int totalCategories = 0;

			  //for (Map.Entry<String, Pair<Integer,Integer>> label : value.recordLabels.entrySet()) {
			  for (Map.Entry<String, Tuple2<Integer,Integer>> label : value.recordLabels.entrySet()) {

			  	// value.recordLabels.size()
			  	//System.out.println("\t\t " + label.getKey() + ": " + label.getValue().getFirst() + ", " + label.getValue().getSecond());
				  System.out.println("\t\t " + label.getKey() + ": " + label.getValue()._1() + ", " + label.getValue()._2());

			  	//totalCategories += label.getValue().getSecond();
				  totalCategories += label.getValue()._2();
			  	
			  }
			  
			  System.out.println("\t\tTotal Categories: " + totalCategories);
			  
			  System.out.println("\t\tMissed Category Lookups: " + value.missedLabelLookups);				  
			  
			  System.out.println("\t\tTotal Values Seen: " + value.count);
			  
		  } else {

			    System.out.println("\t\tmin: " + value.minValue);
			    System.out.println("\t\tmax: " + value.maxValue);
			    System.out.println("\t\tsum: " + value.sum);
			    System.out.println("\t\tcount: " + value.count);
			    System.out.println("\t\tavg: " + value.avg);
			    System.out.println("\t\tvariance: " + value.variance);
			    System.out.println("\t\tstddev: " + value.stddev);

		    }

		}

		System.out.println("End Print Schema --------\n\n");

	}

	public void debugPrintColumns() {

		for (Map.Entry<String, CSVSchemaColumn> entry : this.columnSchemas.entrySet()) {

			String key = entry.getKey();
		    CSVSchemaColumn value = entry.getValue();

		    // now work with key and value...

		//    log.debug("> {} , {} , {}", value.name, value.columnType, value.transform);

		}

	}
	
	public String vectorizePlainCSV( String csvLine ) {
		
		
		StringBuilder sb = new StringBuilder();
        boolean first = true;
        /*
        for(Writable w : c){
            if(!first) sb.append(delim);
            sb.append(w.toString());
            first = false;
        }
        */
        
        double label = 0;
        
    	String[] columns = csvLine.split( this.delimiter );
		
    	if (columns[0].trim().equals("")) {

    		
    	} else {
    		
    		int srcColIndex = 0;
		    int dstColIndex = 0;

		    //log.info( "> Engine.vectorize() ----- ");

		        		
    	
		    double valueTmp = 0;
	
	    	// FOR EACH KEY in the column set
	        // scan through the columns in the schema / input csv data
	        for (Map.Entry<String, CSVSchemaColumn> entry : this.getColumnSchemas().entrySet()) {
	
	          CSVSchemaColumn schemaColumnEntry = entry.getValue();
	          	          
	          if (schemaColumnEntry.transform == CSVSchemaColumn.TransformType.SKIP) {
	        	  
	        	  srcColIndex++;
	        	  
	          } else if (schemaColumnEntry.transform == CSVSchemaColumn.TransformType.LABEL) {
	        	  
	        	  label = schemaColumnEntry.transformColumnValue( columns[ srcColIndex ] );
		          srcColIndex++;
		          dstColIndex++;
	        	  
	          } else {
	        	  valueTmp = schemaColumnEntry.transformColumnValue( columns[ srcColIndex ] );
	        	  // only for sparse formats
	        	  //if (valueTmp > 0.0) {
	        		  
	        	  
	        		  if (dstColIndex != 0) {
	        			  sb.append(",");
	        		  }
	        		  sb.append(  valueTmp );
	        		  
	        	  //}
		          srcColIndex++;
		          dstColIndex++;
	        	  
	          } // if
	          
	          
	        } // for
	        
    	} // if
    	
	
	        
	        // SVMLight format: <label> <index-1>:<value> 2:<value> (...) N:<value>
	        //outputLine = sb.toString() + "," + label ;        

    	String outputLine = sb.toString() + "," + label; 
    	
        return outputLine; 	
		
	}
	
	
	public String vectorizeSVMLightCSVFormat(String csvLine) {
		
		
		String outputLine = "";
		StringBuilder outBuilder = new StringBuilder("");
		
    	//String csvLine = value.toString();
    	String[] columns = csvLine.split( this.delimiter );
		
    	if (columns[0].trim().equals("")) {

    		
    	} else {
    		
    		int srcColIndex = 0;
		    int dstColIndex = 1;

		    //log.info( "> Engine.vectorize() ----- ");

		    double label = 0;    		
    	
		    double valueTmp = 0;
	
	    	// FOR EACH KEY in the column set
	        // scan through the columns in the schema / input csv data
	        for (Map.Entry<String, CSVSchemaColumn> entry : this.getColumnSchemas().entrySet()) {
	
	          CSVSchemaColumn schemaColumnEntry = entry.getValue();
	          	          
	          if (schemaColumnEntry.transform == CSVSchemaColumn.TransformType.SKIP) {
	        	  
	        	  srcColIndex++;
	        	  
	          } else if (schemaColumnEntry.transform == CSVSchemaColumn.TransformType.LABEL) {
	        	  
	        	  label = schemaColumnEntry.transformColumnValue( columns[ srcColIndex ] );
		          srcColIndex++;
		          dstColIndex++;
	        	  
	          } else {
	        	  valueTmp = schemaColumnEntry.transformColumnValue( columns[ srcColIndex ] );
	        	  if (valueTmp > 0.0) {
	        		  outBuilder.append( " " + dstColIndex + ":" + valueTmp );
	        	  }
		          srcColIndex++;
		          dstColIndex++;
	        	  
	          }
	          
	          
	        }
	        
	        // SVMLight format: <label> <index-1>:<value> 2:<value> (...) N:<value>
	        outputLine = label + outBuilder.toString(); 
	        
	        
    	}    	
		
		return outputLine;
		
	}		
		
		
	
	
}
