package org.deeplearning4j.examples.utils;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import lombok.NonNull;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.mllib.linalg.DenseVector;
import org.apache.spark.mllib.linalg.Vectors;
import org.deeplearning4j.nn.conf.MultiLayerConfiguration;
import org.deeplearning4j.nn.multilayer.MultiLayerNetwork;
import org.deeplearning4j.util.ModelSerializer;
import org.nd4j.linalg.api.ndarray.INDArray;
import org.nd4j.linalg.factory.Nd4j;
import org.deeplearning4j.nn.api.Layer;
import org.deeplearning4j.nn.api.Model;
import org.deeplearning4j.nn.api.Updater;


public class Utils {


	public static String getLabel(String SVMLightRecord) {
		
    	String work = SVMLightRecord.trim();
    	int firstSpaceIndex = work.indexOf(' ');
    	String label = work.substring( 0, firstSpaceIndex );
    	
    	return label.trim();
		
		
	}

	
	public static DenseVector convert_CSV_To_Dense_Vector(String rawLine, int size) {
		

		
		String[] parts = rawLine.trim().split(",");
		double[] values = new double[ size ]; //[ parts.length - 1 ];
		
		
		for ( int x = 1; x < size + 1; x++ ) {
			
			
			
			//String[] indexValueParts = parts[ currentPartsIndex ].split(":");
			double parsedValue = Double.parseDouble( parts[ x ] );
			values[ x - 1 ] = parsedValue;

			
			//System.out.println( " x = " + x + " -> " + values[ x - 1 ] + ", "+ parts[ x ]);
			
			
		}
		
		// Vectors.dense(1.0, 0.0, 3.0)
		//return new SparseVector(size, indicies, values);
		return (DenseVector) Vectors.dense( values );
		
	}	
	
	public static MultiLayerNetwork loadDL4JModelFromHDFS( FileSystem hdfs, String hdfsPathToSavedModelFile ) throws IOException {
        
		System.out.println( "Loading Model From HDFS: " + hdfsPathToSavedModelFile );
		
		InputStream hdfs_input_stream = hdfs.open( new Path( hdfsPathToSavedModelFile ) );
		
		//ByteArrayInputStream bInput = new ByteArrayInputStream( hdfs_input_stream );
		
		
		//try (BufferedReader br = new BufferedReader(new InputStreamReader( hdfs_input_stream ))) {
		    //for (String line; (line = br.readLine()) != null; ) {

		    	//b.append(line + "\n");
		    	
		    //}
			//br.
		    // line is not visible here.
		//}
		
		/*
		long byteCount = 0;
		while (hdfs_input_stream.read() != -1) {
			byteCount++;
		}
		
		//int length = hdfs_input_stream.available();
		
		
		
		System.out.println( "Loaded " + byteCount + " bytes");
		
		//System
		
		hdfs_input_stream.reset();
		*/
		
		File tempFile = File.createTempFile("dl4j_tmp", null);
	    tempFile.deleteOnExit();
	    FileOutputStream out = new FileOutputStream(tempFile);
	    IOUtils.copy(hdfs_input_stream, out);
	    //return tempFile;		
		
		//MultiLayerNetwork network = restoreMultiLayerNetwork( hdfs_input_stream );
		MultiLayerNetwork network = ModelSerializer.restoreMultiLayerNetwork(tempFile);
        
        return network;
		
	}
	
	
	public static void saveDL4JModelToHDFS(FileSystem hdfs, String hdfsPathToSaveModel, MultiLayerNetwork trainedNetwork) throws IOException {
		
		
        Path modelPath = new Path( hdfsPathToSaveModel );
        
        if ( hdfs.exists( modelPath )) {
        	hdfs.delete( modelPath, true ); 
        } 
        
        OutputStream os = hdfs.create( modelPath );
        
        BufferedOutputStream stream = new BufferedOutputStream( os );
		
		/*
		// hdfsPathToSaveJSON
        BufferedWriter br = new BufferedWriter( new OutputStreamWriter( os, "UTF-8" ) );
        br.write( networkArchitectureJSON );
        br.close();		
        
        
        // now save the model
		
        modelPath = new Path( hdfsPathToSaveModel );
        
        if ( hdfs.exists( modelPath )) {
        	hdfs.delete( modelPath, true ); 
        } 
        
        os = hdfs.create( modelPath );

        
        DataOutputStream dos = new DataOutputStream( os );
        
        //BufferedOutputStream bos = new BufferedOutputStream( os );
        //Nd4j.write( dos,trainedNetwork.params() );
        Nd4j.write( trainedNetwork.params(), dos );
        
      //  FileUtils.write(new File("conf.yaml"),trainedNetwork.conf().toYaml());
      //  System.out.println("F1 = " + precision);
        dos.close();
        hdfs.close();
        */
        
        ModelSerializer.writeModel( trainedNetwork, stream, true );
        
        os.close();
        
        System.out.println( "Saving model parameters to: " + modelPath );
        //System.out.println( "Associated Model Conf: " + hdfsPathToSaveJSON );        
		
	}
	
	public static void writeLinesToLocalDisk(String path, List<String> lines) {
		
		BufferedWriter writer = null;	
		
		try {
            //create a temporary file
            //String timeLog = new SimpleDateFormat("yyyyMMdd_HHmmss").format(Calendar.getInstance().getTime());
            File logFile = new File( path );

            // This will output the full path where the file will be written to...
            System.out.println(logFile.getCanonicalPath());

            writer = new BufferedWriter(new FileWriter(logFile));
            
    		for (int x = 0; x < lines.size(); x++ ) {
    			
    			//br.writeUTF( lines.get(x) + "\n" );
    			writer.write( lines.get(x) + "\n" );
    			
    		}
    		
    		writer.close();
            
            
            //writer.write("Hello world!");
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                // Close the writer regardless of what happens...
                writer.close();
            } catch (Exception e) {
            }
        }		
		
		
	}
	
	public static void saveLossScoresToHDFS( ) {
		
		
		
	}
	
	public static String loadTextFileContentsAsString( FileSystem fs, String filename ) throws IOException {

		StringBuilder b = new StringBuilder();
		
		//FileSystem fs = FileSystem.get(conf);
		
		InputStream in = null;
		try {
			//in = new FileInputStream( confFilePath );
			in = fs.open( new Path(filename) );
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return "";
		}

		
		//try (BufferedReader br = new BufferedReader(new FileReader(filename))) {
		try (BufferedReader br = new BufferedReader(new InputStreamReader(in))) {
		    for (String line; (line = br.readLine()) != null; ) {

		    	b.append(line + "\n");
		    	
		    }
		    // line is not visible here.
		}
		
		
		
		//conf.set( confKey, b.toString() );
		in.close();

		return b.toString();
	}		
	
	
	

    /**
     * Load a multi layer network
     * from a file
     * @param is the inputstream to load from
     * @return the loaded multi layer network
     * @throws IOException
     */
    public static MultiLayerNetwork restoreMultiLayerNetwork(@NonNull InputStream is) throws IOException {
        ZipInputStream zipFile = new ZipInputStream(is);

        boolean gotConfig = false;
        boolean gotCoefficients = false;
        boolean gotUpdater = false;

        String json = "";
        INDArray params = null;
        Updater updater = null;
        
        System.out.println( "data available in zipfile: " + zipFile.available() );


        ZipEntry entry;
        while((entry = zipFile.getNextEntry()) != null) {
            switch (entry.getName()) {
                case "configuration.json":
                    DataInputStream dis = new DataInputStream(zipFile);
                    params = Nd4j.read(dis);
                    gotCoefficients = true;
                    break;
                case "coefficients.bin":
                    DataInputStream dis2 = new DataInputStream(zipFile);
                    params = Nd4j.read(dis2);
                    gotCoefficients = true;
                    break;
                case "updater.bin":
                    ObjectInputStream ois = new ObjectInputStream(zipFile);

                    try {
                        updater = (Updater) ois.readObject();
                    } catch (ClassNotFoundException e) {
                        throw new RuntimeException(e);
                    }

                    gotUpdater = true;
                    break;

            }

            zipFile.closeEntry();

        }


        zipFile.close();

        if (gotConfig && gotCoefficients) {
            MultiLayerConfiguration confFromJson = MultiLayerConfiguration.fromJson(json);
            MultiLayerNetwork network = new MultiLayerNetwork(confFromJson);
            network.init();
            network.setParameters(params);


            if (gotUpdater && updater != null) {
                network.setUpdater(updater);
            }
            return network;
        } else throw new IllegalStateException("Model wasnt found within file: gotConfig: ["+ gotConfig+"], gotCoefficients: ["+ gotCoefficients+"], gotUpdater: ["+gotUpdater+"]");
    }	
	
	
}
