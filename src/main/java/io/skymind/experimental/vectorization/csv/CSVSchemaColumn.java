package io.skymind.experimental.vectorization.csv;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.LinkedHashMap;
import java.util.Map;

//import org.apache.commons.math3.util.Pair;
import scala.Tuple2;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


public class CSVSchemaColumn  implements java.io.Serializable {


	public enum ColumnType { NUMERIC, DATE, NOMINAL }
	public enum TransformType { COPY, SKIP, BINARIZE, NORMALIZE, LABEL, UNIQUE_ID, ZEROMEAN_ZEROUNITVARIANCE }

	public String name = ""; // the name of the attribute/column
	public ColumnType columnType = null;
	public TransformType transform = null; 

	/*
	 * TODO:
	 * - how do we model statistics per column?
	 * 
	 */
	public double minValue = Double.NaN;
	public double maxValue = Double.NaN;
	public double sum = 0.0;
	public int count = 0;
	
	public double avg = Double.NaN;
	public double variance = Double.NaN;
	public double varianceTmpSum = 0;
	public double stddev = Double.NaN;
	
	//public double stddev = 0;
	//public double median = 0;
	
	// used to track input values that do not match the schema data type
	public long invalidDataEntries = 0;
	public long missedLabelLookups = 0;
	

	// we want to track the label counts to understand the class balance
	// layout: { columnName, columnID, occurenceCount }
	//public Map<String, Pair<Integer, Integer>> recordLabels = new LinkedHashMap<>();
	public Map<String, Tuple2<Integer, Integer>> recordLabels = new LinkedHashMap<>();
	
	
	public CSVSchemaColumn(String colName, ColumnType colType, TransformType transformType) {
		
		this.name = colName;
		this.columnType = colType;
		this.transform = transformType;
		
	}
	
	public void mergePartialDerivedStats(CSVSchemaColumn col) {
		
		if ( ColumnType.NUMERIC == this.columnType   ) {
			
			// varianceTmpSum
			if (Double.isNaN( col.varianceTmpSum )) {
				
				// nothing, use the local sum and go
				
			} else if ( Double.isNaN( this.varianceTmpSum ) ) {
				
				this.varianceTmpSum = col.varianceTmpSum;
				
			} else {
				
				this.varianceTmpSum += col.varianceTmpSum;
				
			}
			
		}
		
		
	}
	
	public void mergePartialCollectStats(CSVSchemaColumn col) {
		
		if ( ColumnType.NUMERIC == this.columnType   ) {
		//this.
			
			// 1.) check min
			if ( Double.isNaN( col.minValue ) ) {
				
				// cant add the other col val in, its NaN
				
			} else if ( Double.isNaN( this.minValue ) ) {
				
				this.minValue = col.minValue;
				
			} else if (col.minValue < this.minValue) {
				
				this.minValue = col.minValue;
				
			}
			
			// 2.) check max

			if ( Double.isNaN( col.maxValue ) ) {
				
			} else if ( Double.isNaN( this.maxValue ) ) {
				
				this.maxValue = col.maxValue;
				
				
			} else if (col.maxValue > this.maxValue) {
				
				this.maxValue = col.maxValue;
				
				
				
			}
			
			
			// 3.) add sum
			
			if ( Double.isNaN( col.sum ) ) {
				
			} else if ( Double.isNaN( this.sum ) ) {
				
				this.sum = col.sum;
				
			} else {
				
				this.sum += col.sum;
				
				
			}
			
			
			
		} else if ( ColumnType.NOMINAL == this.columnType   ) {
			
			// merge the label list + counts
			
			// iterate through all of the new column keys and pull them over
			
			for (Map.Entry<String, Tuple2<Integer,Integer>> entry : col.recordLabels.entrySet()) {
			    
				String key = entry.getKey();
				Tuple2<Integer,Integer> value = entry.getValue();
			    
//			    System.out.println( "> " + key + ", " + value);
				
				String trimmedKey = key.trim();
				
				// then we want to track the record label
				if ( this.recordLabels.containsKey( trimmedKey ) ) {
					
					//System.out.println( " size: " + this.recordLabels.size() );
				
					
					// get the count from the other column first:
					//Integer countPartialInt = col.recordLabels.get( trimmedKey ).getSecond();
					Integer countPartialInt = col.recordLabels.get( trimmedKey )._2();
					
					//Integer labelID = this.recordLabels.get( trimmedKey ).getFirst();
					Integer labelID = this.recordLabels.get( trimmedKey )._1();
					//Integer countInt = this.recordLabels.get( trimmedKey ).getSecond();
					Integer countInt = this.recordLabels.get( trimmedKey )._2();
					
					// sum the two counts into one
					countInt += countPartialInt;
					
					// update our master list
					this.recordLabels.put( trimmedKey, new Tuple2<>( labelID, countInt ) );
					
				} else {
					
					// get the count from the other column first:
					//Integer countPartialInt = col.recordLabels.get( trimmedKey ).getSecond();
					Integer countPartialInt = col.recordLabels.get( trimmedKey )._2();
					
					
					Integer labelID = this.recordLabels.size();
					this.recordLabels.put( trimmedKey, new Tuple2<>( labelID, countPartialInt ) );

					
				}				
			    
			}				
			
			
			
		} else {
			
		}
		
		this.count += col.count;
		
	}
	
	/**
	 * This method collects dataset statistics about the column that we'll 
	 * need later to
	 * 1. convert the column based on the requested transforms
	 * 2. report on column specfic statistics to give visibility into the properties of the input dataset
	 * 
	 * @param value
	 * @throws Exception 
	 */
	public void evaluateColumnValue(String value) throws Exception {

		/*
		 * Need to get stats for the following transforms here:
		 * 1. normalize
		 * 2. binarize
		 * 
		 */
		if ( ColumnType.NUMERIC == this.columnType   ) {
			
			// then we want to look at min/max values
			
			double tmpVal = Double.parseDouble(value);
			
			// System.out.println( "converted: " + tmpVal );
			
			if (Double.isNaN(tmpVal)) {
				throw new Exception("The column was defined as Numeric yet could not be parsed as a Double");
			}
			
			if ( Double.isNaN( this.minValue ) ) {
			
				this.minValue = tmpVal;
				
			} else if (tmpVal < this.minValue) {
				
				this.minValue = tmpVal;
				
			}
			
			if ( Double.isNaN( this.maxValue ) ) {
				
				this.maxValue = tmpVal;
				
			} else if (tmpVal > this.maxValue) {
				
				this.maxValue = tmpVal;
				
			}
			
			this.sum += tmpVal;
			
			//System.out.println( "sum: " + this.sum + " value = " + value + ", tmpVal = " + tmpVal );
			
			
		} else if ( ColumnType.NOMINAL == this.columnType   ) {
			
			// now we are dealing w a set of categories of a label
			
		//} else if ( TransformType.LABEL == this.transform ) {
			
		//	System.out.println( "> label '" + value + "' " );
			
			String trimmedKey = value.trim();
			
			// then we want to track the record label
			if ( this.recordLabels.containsKey( trimmedKey ) ) {
				
				//System.out.println( " size: " + this.recordLabels.size() );
				
				//Integer labelID = this.recordLabels.get( trimmedKey ).getFirst();
				Integer labelID = this.recordLabels.get( trimmedKey )._1();
				
				//Integer countInt = this.recordLabels.get( trimmedKey ).getSecond();
				Integer countInt = this.recordLabels.get( trimmedKey )._2();
				countInt++;
				
				//this.recordLabels.put( trimmedKey, new Pair<>( labelID, countInt ) );
				this.recordLabels.put( trimmedKey, new Tuple2<>( labelID, countInt ) );
				
			} else {
				
				Integer labelID = this.recordLabels.size();
				//this.recordLabels.put( trimmedKey, new Pair<>( labelID, 1 ) );
				this.recordLabels.put( trimmedKey, new Tuple2<>( labelID, 1 ) );

			//	System.out.println( ">>> Adding Label: '" + trimmedKey + "' @ " + labelID );
				
			}
			
		}
		
		this.count++;
		
	}
	
	public void evaluateColumnValueDerivedStatistics(String value) throws Exception {

		/*
		 * Need to get stats for the following transforms here:
		 * 1. normalize
		 * 2. binarize
		 * 
		 */
		if ( ColumnType.NUMERIC == this.columnType   ) {
			
			// then we want to look at min/max values
			
			double tmpVal = Double.parseDouble(value);
			
			// System.out.println( "converted: " + tmpVal );
			
			if (Double.isNaN(tmpVal)) {
				throw new Exception("The column was defined as Numeric yet could not be parsed as a Double");
			}

			this.varianceTmpSum += ( this.avg - tmpVal ) * ( this.avg - tmpVal );  
					/// this.count;
			//this.stddev = Math.sqrt(this.variance);
			//System.out.println( tmpVal );
			

			//System.out.println( "tmpVal = " + tmpVal + ", this.avg = " + this.avg + ", this.count = " + this.count + ", this.variance = " + this.variance + ", stddev: " + this.stddev);
			
			
		} else if ( ColumnType.NOMINAL == this.columnType   ) {
			/*
			// now we are dealing w a set of categories of a label
			
		//} else if ( TransformType.LABEL == this.transform ) {
			
		//	System.out.println( "> label '" + value + "' " );
			
			String trimmedKey = value.trim();
			
			// then we want to track the record label
			if ( this.recordLabels.containsKey( trimmedKey ) ) {
				
				//System.out.println( " size: " + this.recordLabels.size() );
				
				Integer labelID = this.recordLabels.get( trimmedKey ).getFirst();
				Integer countInt = this.recordLabels.get( trimmedKey ).getSecond();
				countInt++;
				
				this.recordLabels.put( trimmedKey, new Pair<>( labelID, countInt ) );
				
			} else {
				
				Integer labelID = this.recordLabels.size();
				this.recordLabels.put( trimmedKey, new Pair<>( labelID, 1 ) );

			//	System.out.println( ">>> Adding Label: '" + trimmedKey + "' @ " + labelID );
				
			}
			*/
		}
		
		//this.count++;
		
	}	
	
	public void computeStatistics() {
		
		if ( ColumnType.NUMERIC == this.columnType ) {
			
			this.avg = this.sum / this.count;
			
		} else {
			
			
		}
		
	}

	
	public void computeDerivedStatistics() {
		
		if ( ColumnType.NUMERIC == this.columnType ) {
			
			//this.avg = this.sum / this.count;
			this.variance = this.varianceTmpSum / this.count;
			this.stddev = Math.sqrt(this.variance);
			
		} else {
			
			
		}
		
	}
	
	
	public void debugPrintColumns() {
		
		//for (Map.Entry<String, Pair<Integer,Integer>> entry : this.recordLabels.entrySet()) {
		for (Map.Entry<String, Tuple2<Integer,Integer>> entry : this.recordLabels.entrySet()) {
		    
			String key = entry.getKey();
		    //Integer value = entry.getValue();
			//Pair<Integer,Integer> value = entry.getValue();
			Tuple2<Integer,Integer> value = entry.getValue();
		    
		    System.out.println( "> " + key + ", " + value);
		    
		    // now work with key and value...
		}		
		
	}
	
	public Integer getLabelCount( String label ) {

		if ( this.recordLabels.containsKey(label) ) {
		
			//return this.recordLabels.get( label ).getSecond();
			return this.recordLabels.get( label )._2();
		
		}
		
		return 0;
		
	}

	public Integer getLabelID( String label ) {
		
	//	System.out.println( "getLableID() => '" + label + "' " );
		
		if ( this.recordLabels.containsKey(label) ) {
		
			//return this.recordLabels.get( label ).getFirst();
			return this.recordLabels.get( label )._1();
			
		}
		
		this.missedLabelLookups++;
		
	//	System.out.println( ".getLabelID() >> returning null with size: " + this.recordLabels.size() );
		return null;
		
	}
	
	
	public double transformColumnValue(String inputColumnValue) {

		inputColumnValue = inputColumnValue.replaceAll("\"", "");
		
		//System.out.println( "no quote! " + inputColumnValue );
		
		switch (this.transform) {
			case LABEL:
				return this.label(inputColumnValue);
			case BINARIZE:
				return this.binarize(inputColumnValue);
			case COPY:
				return this.copy(inputColumnValue);
			case UNIQUE_ID:
				return this.copy(inputColumnValue);
			case NORMALIZE:
				return this.normalize(inputColumnValue);
			case ZEROMEAN_ZEROUNITVARIANCE:
				return this.zeromean_zerounitvariance(inputColumnValue);
			case SKIP:
				return 0.0; // but the vector engine has to remove this from output
		}

		return -1.0; // not good
		
	}
	

	public double copy(String inputColumnValue) {
		
		double return_value = 0;
		
		if (this.columnType == ColumnType.NUMERIC) {
			
			return_value = Double.parseDouble( inputColumnValue );
			
		} else {
			
			// In the prep-pass all of the different strings are indexed
			// copies the label index over as-is as a floating point value (1.0, 2.0, 3.0, ... N)
			
			String key = inputColumnValue.trim();
			
			return_value = this.getLabelID( key );
			
			
		}
		
		return return_value;
	}

	/*
	 * Needed Statistics for binarize() - range of values (min, max) - similar
	 * to normalize, but we threshold on 0.5 after normalize
	 */
	public double binarize(String inputColumnValue) {
		
		double val = Double.parseDouble(inputColumnValue);
		
		double range = this.maxValue - this.minValue;
		double midpoint = ( range / 2 ) + this.minValue;
		
		if (val < midpoint) {
			return 0.0;
		}
		
		return 1.0;
		
	}

	/*
	 * Needed Statistics for normalize() - range of values (min, max)
	 * 
	 * 
	 * normalize( x ) = ( x - min ) / range
	 *  
	 */
	public double normalize(String inputColumnValue) {

		double return_value = 0;
		
		if (this.columnType == ColumnType.NUMERIC) {
		
			double val = Double.parseDouble(inputColumnValue);
			
			double range = this.maxValue - this.minValue;
			double normalizedOut = ( val - this.minValue ) / range;
			
			if (0.0 == range) {
				return_value = 0.0;
			} else {
				return_value = normalizedOut;
			}
			
		} else {
			
			// we have a normalized list of labels
			
			String key = inputColumnValue.trim();
			
			double totalLabels = this.recordLabels.size();
			double labelIndex = this.getLabelID( key ) + 1.0;
			
			//System.out.println("Index Label: " + labelIndex); 
			
			return_value = labelIndex / totalLabels;
			
			
		}
		
		return return_value;		
		
	}

	/**
	 * maps to: 
	 * 
	 * http://grepcode.com/file/repo1.maven.org/maven2/org.deeplearning4j/deeplearning4j-core/0.0.3.1/org/deeplearning4j/datasets/DataSet.java#DataSet.normalizeZeroMeanZeroUnitVariance%28%29
	 * 
	 * @param inputColumnValue
	 * @return
	 */
	public double zeromean_zerounitvariance(String inputColumnValue) {

		double return_value = 0;
		
		if (this.columnType == ColumnType.NUMERIC) {
		
			double val = Double.parseDouble(inputColumnValue);
			
			//double range = this.maxValue - this.minValue;
			//double normalizedOut = ( val - this.minValue ) / range;
			
			double valMinusColMean = val - this.avg;
			double addInDivByZeroProtection = this.stddev + 1e-6;
			double retVal = valMinusColMean / addInDivByZeroProtection;
			
			if (0.0 == retVal) {
				return_value = 0.0;
			} else {
				return_value = retVal;
			}
			
		} else {
			
			// we have a normalized list of labels
			
			String key = inputColumnValue.trim();
			
			double totalLabels = this.recordLabels.size();
			double labelIndex = this.getLabelID( key ) + 1.0;
			
			//System.out.println("Index Label: " + labelIndex); 
			
			return_value = labelIndex / totalLabels;
			
			System.out.println( "ZMZUV: Not supported for Labels currently, nornalizing..." );
			
		}
		
		return return_value;		
		
	}	
	
	
	/*
	 * Needed Statistics for label() - count of distinct labels - index of
	 * labels to IDs (hashtable?)
	 */
	public double label(String inputColumnValue) {

		double return_value = 0;

		if (this.columnType == ColumnType.NUMERIC) {
			
			// In this case, same thing as !COPY --- uses input column numbers as the floating point label value
			return_value = Double.parseDouble(inputColumnValue);
			
		} else {

			// its a nominal value in the indexed list -> pull the index, return it as a double
			
			//System.out.println( "Looking for: " +  inputColumnValue.trim() );
			
			// TODO: how do get a numeric index from a list of labels? 
			Integer ID = this.getLabelID( inputColumnValue.trim() );
			
			return_value = ID;
			
		}
		
		
		return return_value;
		
	}
	
	/**
	 * [[[[ DONT USE THIS ]]]]
	 * 
	 * 
	 * 1. Based on this column name, load the correct stats file
	 * 
	 * 2. Based on this column type, load the correct information { NUMERIC, NOMINAL }
	 * 
	 * @param context
	 */
	public void loadPrepPassStatisticsFromDistributedCache(Configuration conf, Path[] localPaths, String basePath) {
		/*
		Path[] localPaths = null; 
				
		try {
			localPaths = context.getLocalCacheFiles();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		*/
		
		String fullPath = basePath + this.name + ".txt-r-00000";
		
		System.out.println( "full path: " + fullPath );
		
		if (this.transform == CSVSchemaColumn.TransformType.UNIQUE_ID) {
			return;
		}
		
		/**
		 * 
		 * WHY are we pulling this from DCache?
		 * 
		 * why not from just straight hdfs?
		 * 
		 */
		if (this.columnType == CSVSchemaColumn.ColumnType.NOMINAL) {
			
			// get labels
			//Path fileLabelsPath = this.getCorrectDCacheFile(localPaths, fullPath);
			//System.out.println( "Column: " + this.name + " > " + fileLabelsPath.toString());
			Path fileLabelsPath = new Path( fullPath );
			try {
				this.loadLabels( conf, fileLabelsPath );
			} catch (IllegalArgumentException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (NullPointerException e) {
				
			}
			
		} else if (this.columnType == CSVSchemaColumn.ColumnType.NUMERIC) {
			
			// get column stats
			//Path fileStatsPath = this.getCorrectDCacheFile(localPaths, fullPath);
			//System.out.println( "Column: " + this.name + " > " + fileStatsPath.toString());
			
			Path fileStatsPath = new Path( fullPath );
			try {
				this.loadColumnStatistics( conf, fileStatsPath );
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (NullPointerException e) {
				
			}
			
		}
		
		
	}
	
	/**
	 * [[[[ DONT USE THIS ]]]]
	 * 
	 * TODO: TECHNICAL DEBT
	 * 
	 * AND BUG ---- the fullpath variable is not looking in the right place in hdfs --- its looking in the local tmp path!
	 * 
	 * @param localPaths
	 * @param basePath
	 */
	public void loadPrepPassDerivedStatisticsFromDistributedCache(Configuration conf, Path[] localPaths, String basePath) {
		
		String fullPath = basePath + this.name + "_derived" + ".txt-r-00000";
		
		System.out.println( "full path: " + fullPath );
		
		if (this.transform == CSVSchemaColumn.TransformType.UNIQUE_ID) {
			return;
		}
		
		/**
		 * 
		 * WHY are we pulling this from DCache?
		 * 
		 * why not from just straight hdfs?
		 * 
		 */
		if (this.columnType == CSVSchemaColumn.ColumnType.NOMINAL) {
			
			// get labels
			//Path fileLabelsPath = this.getCorrectDCacheFile(localPaths, fullPath);
			//System.out.println( "Column: " + this.name + " > " + fileLabelsPath.toString());
		//	Path fileLabelsPath = new Path( fullPath );
			//this.loadLabels(fileLabelsPath);
			
			// do nothing!
			
		} else if (this.columnType == CSVSchemaColumn.ColumnType.NUMERIC) {
			
			// get column stats
			//Path fileStatsPath = this.getCorrectDCacheFile(localPaths, fullPath);
			//System.out.println( "Column: " + this.name + " > " + fileStatsPath.toString());
			
			Path fileStatsPath = new Path( fullPath );
			try {
				this.loadColumnDerivedStatistics(conf, fileStatsPath);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (NullPointerException e) {
				
			}
			
		}
		
		
	}
	
	/**
	 * 
	 * 
{id},{label_string},{count}

0,Iris-setosa,3
1,Iris-versicolor,5
2,Iris-virginica,4

	 * 
	 * @param context
	 * @param filePath
	 * @throws IOException 
	 * @throws IllegalArgumentException 
	 */
	private void loadLabels(Configuration conf, Path filePath) throws IllegalArgumentException, IOException {
		
		//Configuration conf = context.getConfiguration();
		
		
		System.out.println( "> Loading Labels For: " + this.name );
		
		this.recordLabels.clear();
		
		FileSystem fs = FileSystem.get(conf);
		
		InputStream in = null;
		try {
			//in = new FileInputStream( confFilePath );
			in = fs.open( new Path( filePath.toString() ) );
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
    	//try (BufferedReader br = new BufferedReader(new FileReader(filePath.toString()))) {
		try (BufferedReader br = new BufferedReader(new InputStreamReader(in))) {
    	    String line;
    	    while ((line = br.readLine()) != null) {
    	       
    	    	if ("".equals(line.trim())) {
    	    		// blank!
    	    	} else {
    	    		String[] parts = line.split(",");
    	    		//System.out.println( "Conf > " + parts[ 0 ] + " => " + parts[ 1 ] );
    	    		//conf.set(parts[0].trim(), parts[1].trim());
    	    		
    				Integer labelID = Integer.parseInt( parts[ 0 ] );
    				String trimmedKey = parts[ 1 ].trim();
    				int count = Integer.parseInt( parts[ 2 ] );
    				this.count += count;
    				
    				//this.recordLabels.put( trimmedKey, new Pair<>( labelID, count ) );
    				this.recordLabels.put( trimmedKey, new Tuple2<>( labelID, count ) );
    	    		
    	    		
    	    	}
    	    	
    	    }
    	} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 	
    	in.close();
    //	this.debugPrintColumns();
		
	}
	
	/**
	 *
	 * Example:
	 
canova.data.column.statistics.minimum.petallength=1.3
canova.data.column.statistics.maximum.petallength=6.0

	 * 
	 * @param context
	 * @param filePath
	 * @throws IOException 
	 */
	private void loadColumnStatistics(Configuration conf, Path filePath) throws IOException {
		
		String columnMinKey = "canova.data.column.statistics.minimum." + this.name;
		String columnMaxKey = "canova.data.column.statistics.maximum." + this.name;
		
		String columnCountKey = "canova.data.column.statistics.count." + this.name;
		String columnSumKey = "canova.data.column.statistics.sum." + this.name;
		
		//Configuration conf = context.getConfiguration();
		
		//System.out.println( "Whaaaaaaaaat\n\n" );
		
		System.out.println( "> Loading Column Statistics For: " + this.name );
		
		//this.recordLabels.clear();
		
		//System.out.println( "attempting to load file: " + filePath.toString() );

		FileSystem fs = FileSystem.get(conf);
		
		InputStream in = null;
		try {
			//in = new FileInputStream( confFilePath );
			in = fs.open( new Path( filePath.toString() ) );
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
    	//try (BufferedReader br = new BufferedReader(new FileReader(filePath.toString()))) {
		try (BufferedReader br = new BufferedReader(new InputStreamReader(in))) {		
    	//try (BufferedReader br = new BufferedReader(new FileReader(filePath.toString()))) {
    	    String line;
    	    while ((line = br.readLine()) != null) {
    	       
    	    	System.out.println( "line: " + line );
    	    	// split line into K=V parts
    	    	
    	    	String[] lineParts = line.split( "=" );
    	    	
    	    	if ("".equals(line.trim())) {
    	    		// blank!
    	    	} else if (columnMinKey.equals(lineParts[0].trim())) {
    	    		
    	    		this.minValue = Double.parseDouble( lineParts[ 1 ] );
    	    		
    	    	} else if (columnMaxKey.equals(lineParts[0].trim())) {
    	    		
    	    		this.maxValue = Double.parseDouble( lineParts[ 1 ] );

    	    	} else if (columnCountKey.equals(lineParts[0].trim())) {
    	    		
    	    		this.count = Integer.parseInt( lineParts[ 1 ] );
    	    		
    	    	} else if (columnSumKey.equals(lineParts[0].trim())) {
    	    		
    	    		this.sum = Double.parseDouble( lineParts[ 1 ] );
    	    		
    	    		
    	    	} else {

    	    		// non-recognized key!
    	    		System.out.println( "Error: Did not recognize KV entry: " + line );
    	    		
    	    	}
    	    	
    	    }
    	} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 	
		
		in.close();
    	
    	//this.debugPrintColumns();
    	System.out.println( "Min: " + this.minValue );
    	System.out.println( "Max: " + this.maxValue );
//    	System.out.println( "Max: " + this. );
    	
		
	}	
	
	private void loadColumnDerivedStatistics(Configuration conf,Path filePath) throws IOException {
		
		//String columnMinKey = "canova.data.column.statistics.minimum." + this.name;
		//String columnMaxKey = "canova.data.column.statistics.maximum." + this.name;
		
//		String columnCountKey = "canova.data.column.statistics.count." + this.name;
	//	String columnSumKey = "canova.data.column.statistics.sum." + this.name;
		
		String columnVarianceKey = "canova.data.column.statistics.variance." + this.name;
		String columnSTDDEVKey = "canova.data.column.statistics.stddev." + this.name;
		
		System.out.println( "> Loading Column Derived Statistics For: " + this.name );
		
		//this.recordLabels.clear();
		
		System.out.println( "attempting to load file: " + filePath.toString() );
		
		

		FileSystem fs = FileSystem.get(conf);
		
		InputStream in = null;
		try {
			//in = new FileInputStream( confFilePath );
			in = fs.open( new Path( filePath.toString() ) );
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
    	//try (BufferedReader br = new BufferedReader(new FileReader(filePath.toString()))) {
		try (BufferedReader br = new BufferedReader(new InputStreamReader(in))) {		
    	//try (BufferedReader br = new BufferedReader(new FileReader(filePath.toString()))) {
    	    String line;
    	    while ((line = br.readLine()) != null) {
    	       
    	    	System.out.println( "line: " + line );
    	    	// split line into K=V parts
    	    	
    	    	String[] lineParts = line.split( "=" );
    	    	
    	    	if ("".equals(line.trim())) {
    	    		// blank!
    	    	} else if (columnVarianceKey.equals(lineParts[0].trim())) {
    	    		
    	    		this.variance = Double.parseDouble( lineParts[ 1 ] );
    	    		
    	    	} else if (columnSTDDEVKey.equals(lineParts[0].trim())) {
    	    		
    	    		this.stddev = Double.parseDouble( lineParts[ 1 ] );

    	    	} else {

    	    		// non-recognized key!
    	    		System.out.println( "Error: Did not recognize KV entry: " + line );
    	    		
    	    	}
    	    	
    	    }
    	} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 	
    	
    	//this.debugPrintColumns();
    	System.out.println( "Variance: " + this.variance );
    	System.out.println( "STDDEV: " + this.stddev );
    	
		
	}
	
	private Path getCorrectDCacheFile(Path[] paths, String searchPath) {
		
		Path out = null;
		
		for (int x = 0; x < paths.length; x++ ) {
			
			String[] parts = paths[ x ].toString().split("/");
			String filenameInDCache = parts[ parts.length - 1 ];
			
			
			if (searchPath.trim().equals( filenameInDCache )) {
				out = paths[x];
				break;
			}
			
		}
		
		return out;
		
	}
	
		
	
}
