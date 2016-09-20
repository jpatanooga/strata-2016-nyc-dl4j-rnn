package org.deeplearning4j.examples.rnn.beer;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;

import org.apache.commons.io.FileUtils;
import org.deeplearning4j.examples.rnn.beer.utils.EpochScoreTracker;
//import org.deeplearning4j.examples.rnn.shakespeare.CharacterIterator;
import org.deeplearning4j.nn.api.Layer;
import org.deeplearning4j.nn.api.OptimizationAlgorithm;
import org.deeplearning4j.nn.conf.BackpropType;
import org.deeplearning4j.nn.conf.MultiLayerConfiguration;
import org.deeplearning4j.nn.conf.NeuralNetConfiguration;
import org.deeplearning4j.nn.conf.Updater;
import org.deeplearning4j.nn.conf.distribution.UniformDistribution;
import org.deeplearning4j.nn.conf.layers.GravesLSTM;
import org.deeplearning4j.nn.conf.layers.RnnOutputLayer;
import org.deeplearning4j.nn.multilayer.MultiLayerNetwork;
import org.deeplearning4j.nn.weights.WeightInit;
import org.deeplearning4j.optimize.listeners.CollectScoresIterationListener;
import org.deeplearning4j.optimize.listeners.ScoreIterationListener;
import org.deeplearning4j.util.ModelSerializer;
import org.nd4j.linalg.api.ndarray.INDArray;
import org.nd4j.linalg.factory.Nd4j;
import org.nd4j.linalg.lossfunctions.LossFunctions.LossFunction;

public class LSTMBeerReviewModelingExample {
	public static void main( String[] args ) throws Exception {
		int lstmLayerSize = 200;					//Number of units in each GravesLSTM layer
		int miniBatchSize = 40;						//Size of mini batch to use when  training
		
		int reviewsCoreTrainCount = 242935;
		
		//int examplesPerEpoch = 300 * miniBatchSize;	//i.e., how many examples to learn on between generating samples
		
		int examplesPerEpoch = 4000; //240000;	//i.e., how many examples to learn on between generating samples
		
		int tbpttLength = 50;                       //Length for truncated backpropagation through time. i.e., do parameter updates ever 50 characters
		
		int exampleLength = 100;					//Length of each training example
		int numEpochs = 20;							//Total number of training + sample generation epochs
		int nSamplesToGenerate = 4;					//Number of samples to generate after each training epoch
		int nCharactersToSample = 100;				//Length of each sample to generate
		String generationInitialization = "the";		//Optional character initialization; a random character is used if null
		// Above is Used to 'prime' the LSTM with a character sequence to continue/complete.
		// Initialization characters must all be in CharacterIterator.getMinimalCharacterSet() by default
		Random rng = new Random(12345);
		
		//Get a DataSetIterator that handles vectorization of text into something we can use to train
		// our GravesLSTM network.
		//String dataPath = "/Users/josh/Documents/Talks/2016/Strata_NYC/data/beer/simple_reviews_debug.json";
		
		// Count: 242,935
		String dataPath = "/Users/josh/Documents/Talks/2016/Strata_NYC/data/beer/reviews_core-train.json";
		
		String pathToBeerData = "/Users/josh/Documents/Talks/2016/Strata_NYC/data/beer/beers_all.json";
		
		
		String modelSavePath = "/Users/josh/Documents/workspace/Skymind/talks/models/beer_review/dl4j_beer_review_strata.model";
		boolean loadPrevModel = true;
		String modelLoadPath = modelSavePath;
		
		EpochScoreTracker tracker = new EpochScoreTracker();
		
		BeerReviewCharacterIterator iter = getBeerReviewIterator(miniBatchSize,exampleLength,examplesPerEpoch, dataPath, pathToBeerData);
		int nOut = iter.totalOutcomes();
		/*
		//Set up network configuration:
		MultiLayerConfiguration conf = new NeuralNetConfiguration.Builder()
			.optimizationAlgo(OptimizationAlgorithm.STOCHASTIC_GRADIENT_DESCENT).iterations(1)
			.learningRate(0.1)
			.rmsDecay(0.95)
			.seed(12345)
			.regularization(true)
			.l2(0.001)
			.layer( new GravesLSTM.Builder().nIn(iter.inputColumns()).nOut(lstmLayerSize)
					.updater(Updater.RMSPROP)
					.activation("tanh").weightInit(WeightInit.DISTRIBUTION)
					.dist(new UniformDistribution(-0.08, 0.08)).build())
			.layer( new GravesLSTM.Builder().nIn(lstmLayerSize).nOut(lstmLayerSize)
					.updater(Updater.RMSPROP)
					.activation("tanh").weightInit(WeightInit.DISTRIBUTION)
					.dist(new UniformDistribution(-0.08, 0.08)).build())
			.layer( new RnnOutputLayer.Builder(LossFunction.MCXENT).activation("softmax")        //MCXENT + softmax for classification
					.updater(Updater.RMSPROP)
					.nIn(lstmLayerSize).nOut(nOut).weightInit(WeightInit.DISTRIBUTION)
					.dist(new UniformDistribution(-0.08, 0.08)).build())
			
			.build();
		*/
		
		

		//Set up network configuration:
		MultiLayerConfiguration conf = new NeuralNetConfiguration.Builder()
			.optimizationAlgo(OptimizationAlgorithm.STOCHASTIC_GRADIENT_DESCENT).iterations(1)
			.learningRate(0.1)
			.rmsDecay(0.95)
			.seed(12345)
			.regularization(true)
			.l2(0.001)
            .weightInit(WeightInit.XAVIER)
            .updater(Updater.RMSPROP)
			.list()
			.layer(0, new GravesLSTM.Builder().nIn(iter.inputColumns()).nOut(lstmLayerSize)
					.activation("tanh").build())
			.layer(1, new GravesLSTM.Builder().nIn(lstmLayerSize).nOut(lstmLayerSize)
					.activation("tanh").build())
			.layer(2, new RnnOutputLayer.Builder(LossFunction.MCXENT).activation("softmax")        //MCXENT + softmax for classification
					.nIn(lstmLayerSize).nOut(nOut).build())
            .backpropType(BackpropType.TruncatedBPTT).tBPTTForwardLength(tbpttLength).tBPTTBackwardLength(tbpttLength)
			.pretrain(false).backprop(true)
			.build();		
		
		MultiLayerNetwork net = new MultiLayerNetwork(conf);
		
		if (loadPrevModel) {
			
			System.out.println("Loading existing model for continued training from local disk: " + modelLoadPath );
			net = loadModelFromLocalDisk( modelLoadPath );
			
			
		} else {
			
			System.out.println( "Starting with a new model..." );
			
		}

		net.init();
		
		CollectScoresIterationListener scoresCollection = new CollectScoresIterationListener(10);
		//ScoreIterationListener scoresCollection = new ScoreIterationListener( 10 );
		
		net.setListeners( scoresCollection ); //new ScoreIterationListener(1));

		//Print the  number of parameters in the network (and for each layer)
		Layer[] layers = net.getLayers();
		int totalNumParams = 0;
		for( int i=0; i<layers.length; i++ ){
			int nParams = layers[i].numParams();
			System.out.println("Number of parameters in layer " + i + ": " + nParams);
			totalNumParams += nParams;
		}
		System.out.println("Total number of network parameters: " + totalNumParams);
		
		long totalExamplesAcrossEpochs = 0;
		long start = System.currentTimeMillis();
		long totalTrainingTimeMS = 0;
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd_HH_mm_ss");

		
		//Do training, and then generate and print samples from network
		for( int i=0; i<numEpochs; i++ ){
			
			start = System.currentTimeMillis();
			
			net.fit(iter);
			
			
			
            long end = System.currentTimeMillis();
            long epochSeconds = Math.abs(end - start) / 1000; // se
            long epochMin = epochSeconds / 60; // se
            
            totalTrainingTimeMS += Math.abs(end - start);
			
			
			totalExamplesAcrossEpochs += examplesPerEpoch;
			
			System.out.println("--------------------");
			System.out.println("Completed epoch " + i );
			
			System.out.println( "Epoch Loss Score: " + net.score() );
			tracker.addScore( net.score() );
			
			System.out.println("Time for Epoch Training " + Math.abs(end - start) + " ms, (" + epochSeconds + " seconds) (" + (epochMin) + " minutes)");
			System.out.println( "Total Training Time so Far: " + (totalTrainingTimeMS / 1000 / 60) + " minutes" );
			// track records
			//System.out.println( "Records in epoch: " + examplesPerEpoch );
			//System.out.println( "Records in dataset: " + iter.totalExamplesinDataset );
			//System.out.println( "Records Seen Across Epochs: " + totalExamplesAcrossEpochs );
			
			//System.out.println("Sampling characters from network given initialization \""+ (generationInitialization == null ? "" : generationInitialization) +"\"");
			//String[] samples = sampleCharactersFromNetwork(generationInitialization,net,iter,rng,nCharactersToSample,nSamplesToGenerate);

			float rating_overall = 4.0f;
			float rating_taste = 5.0f;
			float rating_appearance = 4.0f;
			float rating_palate = 5.0f;
			float rating_aroma = 5.0f;	
			int styleIndex = 2; // Lager
			
			String[] samples = sampleBeerRatingFromNetwork( generationInitialization, net, iter, rng, nCharactersToSample, nSamplesToGenerate, rating_overall, rating_taste, rating_appearance, rating_palate, rating_aroma, styleIndex );
			
			for( int j=0; j<samples.length; j++ ){
				System.out.println("----- Generating Lager Beer Review Sample [" + j + "] -----");
				System.out.println(samples[j]);
				System.out.println();
			}
			
			//net.getListeners()
			
			
			Date now = new Date();
			String strDate = sdf.format(now);
			
			//String path = "/tmp/dl4j_rnn_" + strDate + ".model";
			
			File tempFile = new File("/tmp/dl4j_rnn_" + strDate + ".model");// .createTempFile("tsfs", "fdfsdf");
			
			ModelSerializer.writeModel( net, tempFile, true );
			
			tempFile = new File( modelSavePath );// .createTempFile("tsfs", "fdfsdf");
			
			ModelSerializer.writeModel( net, tempFile, true );

			System.out.println( "Model checkpoint saved to: " + tempFile );
			
			iter.reset();	//Reset iterator for another epoch
		}
		
		System.out.println( "Training Final Report: " );
		System.out.println( "Training First Score: " + tracker.firstScore );
		System.out.println( "Training Average Score: " + tracker.avgScore() );
		System.out.println( "Training Score Improvement: " + tracker.scoreChangeOverWindow() );
		
		
		System.out.println("\n\nExample complete");
		

		
		
	}
	
	public static MultiLayerNetwork loadModelFromLocalDisk(String path) throws IOException {
		
		File file = new File( path );
		
		MultiLayerNetwork network = ModelSerializer.restoreMultiLayerNetwork( file );
		
		return network;
	}
	
	/**
	 * Fix location stuff
	 * 
	 * @param miniBatchSize
	 * @param exampleLength
	 * @param examplesPerEpoch
	 * @return
	 * @throws Exception
	 */
	public static BeerReviewCharacterIterator getBeerReviewIterator(int miniBatchSize, int exampleLength, int examplesPerEpoch, String pathToReviewData, String pathToBeerData) throws Exception{
		//The Complete Works of William Shakespeare
		//5.3MB file in UTF-8 Encoding, ~5.4 million characters
		//https://www.gutenberg.org/ebooks/100
		//String url = "https://s3.amazonaws.com/dl4j-distribution/pg100.txt";
		
		String pathToTestData = "/Users/josh/Documents/Talks/2016/Strata_NYC/data/beer/reviews_top-test.json"; 
		if (null != pathToReviewData) {
			pathToTestData = pathToReviewData;
		}
		//String pathToTestData = "/Users/josh/Documents/Talks/2016/Strata_NYC/data/beer/reviews_top-test.json";
		//String pathToTestData = "/Users/josh/Documents/Talks/2016/Strata_NYC/data/beer/simple_reviews_debug.json";

		//String tempDir = System.getProperty("java.io.tmpdir");
		//String fileLocation = tempDir + "/Shakespeare.txt";	//Storage location from downloaded file
/*
		File f = new File(fileLocation);
		if( !f.exists() ){
			FileUtils.copyURLToFile(new URL(url), f);
			System.out.println("File downloaded to " + f.getAbsolutePath());
		} else {
			System.out.println("Using existing text file at " + f.getAbsolutePath());
		}
		
		if(!f.exists()) throw new IOException("File does not exist: " + fileLocation);	//Download problem?
	*/	
		char[] validCharacters = BeerReviewCharacterIterator.getMinimalCharacterSet();	//Which characters are allowed? Others will be removed
		return new BeerReviewCharacterIterator(pathToTestData, pathToBeerData, Charset.forName("UTF-8"),
				miniBatchSize, exampleLength, examplesPerEpoch, validCharacters, new Random(12345),true);
		
		
		
	}

	/** Downloads Shakespeare training data and stores it locally (temp directory). Then set up and return a simple
	 * DataSetIterator that does vectorization based on the text.
	 * @param miniBatchSize Number of text segments in each training mini-batch
	 * @param exampleLength Number of characters in each text segment.
	 * @param examplesPerEpoch Number of examples we want in an 'epoch'. 
	 */
/*	private static CharacterIterator getShakespeareIterator(int miniBatchSize, int exampleLength, int examplesPerEpoch) throws Exception{
		//The Complete Works of William Shakespeare
		//5.3MB file in UTF-8 Encoding, ~5.4 million characters
		//https://www.gutenberg.org/ebooks/100
		String url = "https://s3.amazonaws.com/dl4j-distribution/pg100.txt";
		String tempDir = System.getProperty("java.io.tmpdir");
		String fileLocation = tempDir + "/Shakespeare.txt";	//Storage location from downloaded file
		File f = new File(fileLocation);
		if( !f.exists() ){
			FileUtils.copyURLToFile(new URL(url), f);
			System.out.println("File downloaded to " + f.getAbsolutePath());
		} else {
			System.out.println("Using existing text file at " + f.getAbsolutePath());
		}
		
		if(!f.exists()) throw new IOException("File does not exist: " + fileLocation);	//Download problem?
		
		char[] validCharacters = CharacterIterator.getMinimalCharacterSet();	//Which characters are allowed? Others will be removed
		return new CharacterIterator(fileLocation, Charset.forName("UTF-8"),
				miniBatchSize, exampleLength, examplesPerEpoch, validCharacters, new Random(12345),true);
	}
	*/
	
	
	// TODO: sampleBeerReview 
	// 			include: { ratings ..., user, beer type }
/*
 * 
					public float rating_overall = 0.0f;
					public float rating_taste = 0.0f;
					public float rating_appearance = 0.0f;
					public float rating_palate = 0.0f;
					public float rating_aroma = 0.0f;

 * 	
 */
	private static String[] sampleBeerRatingFromNetwork( String initialization, MultiLayerNetwork net,
			BeerReviewCharacterIterator iter, Random rng, int charactersToSample, int numSamples, 
			float rating_overall, float rating_taste, float rating_appearance, float rating_palate, float rating_aroma, int styleIndex ){
	
	

		
		
		//Set up initialization. If no initialization: use a random character
		if( initialization == null ){
			initialization = String.valueOf(iter.getRandomCharacter());
		}
		
		
		
		//Create input for initialization
		INDArray initializationInput = Nd4j.zeros(numSamples, iter.inputColumns(), initialization.length());
		char[] init = initialization.toCharArray();
		
		// for each timestep we want to generate a character
		for( int charTimestep = 0; charTimestep < init.length; charTimestep++ ){
		
			// get the column index for the current character
			int col_idx = iter.convertCharacterToIndex( init[ charTimestep ] );
			
			// for each sample
			for( int sampleIndex = 0; sampleIndex < numSamples; sampleIndex++ ){
			
				initializationInput.putScalar(new int[]{ sampleIndex, col_idx, charTimestep }, 1.0f);

				
				// TODO: here is where we need to add in ratings for generation hints
				
				int staticColumnBaseOffset = iter.characterColumns();
				
				initializationInput.putScalar(new int[]{ sampleIndex, staticColumnBaseOffset, charTimestep }, rating_appearance );
				initializationInput.putScalar(new int[]{ sampleIndex, staticColumnBaseOffset + 1, charTimestep }, rating_aroma );
				initializationInput.putScalar(new int[]{ sampleIndex, staticColumnBaseOffset + 2, charTimestep }, rating_overall );
				initializationInput.putScalar(new int[]{ sampleIndex, staticColumnBaseOffset + 3, charTimestep }, rating_palate );
				initializationInput.putScalar(new int[]{ sampleIndex, staticColumnBaseOffset + 4, charTimestep }, rating_taste );

				
				// SETUP STYLE INDEX
				int styleColumnBaseOffset = staticColumnBaseOffset + 5;
				int styleIndexColumn = styleColumnBaseOffset + styleIndex; // add the base to the index
				
				//System.out.println( "style index base: " + styleColumnBaseOffset + ", offset: " + styleIndexColumn );
				
				initializationInput.putScalar(new int[]{ sampleIndex, styleIndexColumn, charTimestep }, 1.0 );
				
				
			}
			
		}
		
		
		
		
		
		// TODO: end ratings hints in input array
		
		
		StringBuilder[] sb = new StringBuilder[numSamples];
		
		for ( int i = 0; i < numSamples; i++ ) {
		
			sb[ i ] = new StringBuilder( initialization );
		
		}
		
		//Sample from network (and feed samples back into input) one character at a time (for all samples)
		//Sampling is done in parallel here
		net.rnnClearPreviousState();
		INDArray output = net.rnnTimeStep( initializationInput );
		output = output.tensorAlongDimension(output.size(2)-1,1,0);	//Gets the last time step output
		
		for( int i=0; i<charactersToSample; i++ ){
			//Set up next input (single time step) by sampling from previous output
			INDArray nextInput = Nd4j.zeros(numSamples,iter.inputColumns());
			//Output is a probability distribution. Sample from this for each example we want to generate, and add it to the new input
			for( int s=0; s<numSamples; s++ ){
				double[] outputProbDistribution = new double[iter.totalOutcomes()];
				for( int j=0; j<outputProbDistribution.length; j++ ) outputProbDistribution[j] = output.getDouble(s,j);
				int sampledCharacterIdx = sampleFromDistribution(outputProbDistribution,rng);
				
				nextInput.putScalar(new int[]{s,sampledCharacterIdx}, 1.0f);		//Prepare next time step input
				sb[s].append(iter.convertIndexToCharacter(sampledCharacterIdx));	//Add sampled character to StringBuilder (human readable output)
			}
			
			output = net.rnnTimeStep(nextInput);	//Do one time step of forward pass
		}
		
		String[] out = new String[numSamples];
		for( int i=0; i<numSamples; i++ ) out[i] = sb[i].toString();
		return out;
	}
	
	
	/** Generate a sample from the network, given an (optional, possibly null) initialization. Initialization
	 * can be used to 'prime' the RNN with a sequence you want to extend/continue.<br>
	 * Note that the initalization is used for all samples
	 * @param initialization String, may be null. If null, select a random character as initialization for all samples
	 * @param charactersToSample Number of characters to sample from network (excluding initialization)
	 * @param net MultiLayerNetwork with one or more GravesLSTM/RNN layers and a softmax output layer
	 * @param iter CharacterIterator. Used for going from indexes back to characters
	 */
	private static String[] sampleCharactersFromNetwork( String initialization, MultiLayerNetwork net,
			BeerReviewCharacterIterator iter, Random rng, int charactersToSample, int numSamples ){
		//Set up initialization. If no initialization: use a random character
		if( initialization == null ){
			initialization = String.valueOf(iter.getRandomCharacter());
		}
		
		//Create input for initialization
		INDArray initializationInput = Nd4j.zeros(numSamples, iter.inputColumns(), initialization.length());
		char[] init = initialization.toCharArray();
		for( int i=0; i<init.length; i++ ){
			int idx = iter.convertCharacterToIndex(init[i]);
			for( int j=0; j<numSamples; j++ ){
				initializationInput.putScalar(new int[]{j,idx,i}, 1.0f);
			}
		}
		
		StringBuilder[] sb = new StringBuilder[numSamples];
		for( int i=0; i<numSamples; i++ ) sb[i] = new StringBuilder(initialization);
		
		//Sample from network (and feed samples back into input) one character at a time (for all samples)
		//Sampling is done in parallel here
		net.rnnClearPreviousState();
		INDArray output = net.rnnTimeStep(initializationInput);
		output = output.tensorAlongDimension(output.size(2)-1,1,0);	//Gets the last time step output
		
		for( int i=0; i<charactersToSample; i++ ){
			//Set up next input (single time step) by sampling from previous output
			INDArray nextInput = Nd4j.zeros(numSamples,iter.inputColumns());
			//Output is a probability distribution. Sample from this for each example we want to generate, and add it to the new input
			for( int s=0; s<numSamples; s++ ){
				double[] outputProbDistribution = new double[iter.totalOutcomes()];
				for( int j=0; j<outputProbDistribution.length; j++ ) outputProbDistribution[j] = output.getDouble(s,j);
				int sampledCharacterIdx = sampleFromDistribution(outputProbDistribution,rng);
				
				nextInput.putScalar(new int[]{s,sampledCharacterIdx}, 1.0f);		//Prepare next time step input
				sb[s].append(iter.convertIndexToCharacter(sampledCharacterIdx));	//Add sampled character to StringBuilder (human readable output)
			}
			
			output = net.rnnTimeStep(nextInput);	//Do one time step of forward pass
		}
		
		String[] out = new String[numSamples];
		for( int i=0; i<numSamples; i++ ) out[i] = sb[i].toString();
		return out;
	}
	
	/** Given a probability distribution over discrete classes, sample from the distribution
	 * and return the generated class index.
	 * @param distribution Probability distribution over classes. Must sum to 1.0
	 */
	private static int sampleFromDistribution( double[] distribution, Random rng ){
		double d = rng.nextDouble();
		double sum = 0.0;
		for( int i=0; i<distribution.length; i++ ){
			sum += distribution[i];
			if( d <= sum ) return i;
		}
		//Should never happen if distribution is a valid probability distribution
		throw new IllegalArgumentException("Distribution is invalid? d="+d+", sum="+sum);
	}
}
