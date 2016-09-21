package org.deeplearning4j.examples.rnn.beer.dev;

import org.deeplearning4j.datasets.datavec.SequenceRecordReaderDataSetIterator;
import org.deeplearning4j.examples.rnn.beer.schema.json.BeerDictionary;
import org.deeplearning4j.examples.rnn.beer.schema.json.BeerReviewReader;
import org.deeplearning4j.examples.rnn.beer.schema.json.BeerReviewSequenceRecordReader;
import org.deeplearning4j.examples.rnn.beer.utils.EpochScoreTracker;
import org.deeplearning4j.examples.utils.Utils;
import org.deeplearning4j.nn.api.Layer;
import org.deeplearning4j.nn.api.OptimizationAlgorithm;
import org.deeplearning4j.nn.conf.BackpropType;
import org.deeplearning4j.nn.conf.MultiLayerConfiguration;
import org.deeplearning4j.nn.conf.NeuralNetConfiguration;
import org.deeplearning4j.nn.conf.Updater;
import org.deeplearning4j.nn.conf.layers.GravesLSTM;
import org.deeplearning4j.nn.conf.layers.RnnOutputLayer;
import org.deeplearning4j.nn.multilayer.MultiLayerNetwork;
import org.deeplearning4j.nn.weights.WeightInit;
import org.deeplearning4j.optimize.listeners.CollectScoresIterationListener;
import org.deeplearning4j.util.ModelSerializer;
import org.nd4j.linalg.api.ndarray.INDArray;
import org.nd4j.linalg.factory.Nd4j;
import org.nd4j.linalg.indexing.INDArrayIndex;
import org.nd4j.linalg.indexing.NDArrayIndex;
import org.nd4j.linalg.lossfunctions.LossFunctions.LossFunction;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;

//import org.deeplearning4j.examples.rnn.shakespeare.CharacterIterator;

public class LSTMBeerReviewModelingExample {
	public static void main( String[] args ) throws Exception {
	    int lstmLayerSize = Integer.parseInt(args[0]);					//Number of units in each GravesLSTM layer
		int miniBatchSize = Integer.parseInt(args[1]);						//Size of mini batch to use when  training
		
		int reviewsCoreTrainCount = 135000;
		
		//int examplesPerEpoch = 300 * miniBatchSize;	//i.e., how many examples to learn on between generating samples
		
		int numExamplesPerEpoch = miniBatchSize * 50; //240000;	//i.e., how many examples to learn on between generating samples
		
		int tbpttLength = 200;                       //Length for truncated backpropagation through time. i.e., do parameter updates ever 50 characters
		
		int numEpochs = Integer.parseInt(args[2]);							//Total number of training + sample generation epochs
		int nCharactersToSample = 5000;				//Length of each sample to generate
		// Above is Used to 'prime' the LSTM with a character sequence to continue/complete.
		// Initialization characters must all be in CharacterIterator.getMinimalCharacterSet() by default
		Random rng = new Random(12345);
		
		//Get a DataSetIterator that handles vectorization of text into something we can use to train
		// our GravesLSTM network.
		//String dataPath = "/Users/josh/Documents/Talks/2016/Strata_NYC/data/beer/simple_reviews_debug.json";
		
		// Count: 242,935
		String dataPath = "/Users/josh/Documents/Talks/2016/Strata_NYC/data/beer/reviews_top-train.json";
		String pathToBeerData = "/Users/josh/Documents/Talks/2016/Strata_NYC/data/beer/beers_all.json";

		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd_HH_mm_ss");

		Date nowLogPath = new Date();
		String strDateLogPath = sdf.format(nowLogPath);

		String baseModelPath = "/Users/josh/Documents/Talks/2016/Strata_NYC/data/models/beer_review/";

		File logDirectory = new File(baseModelPath + "logs/");

		if (logDirectory.exists() && logDirectory.isDirectory()) {

		} else {

			//Files.createDirectory(new Path() );
			logDirectory.mkdirs();
			System.out.println( "Creating log directory: " + logDirectory.toString() );

		}
		
		String extraLogLinesPath = baseModelPath + "logs/dl4j_beer_review_" + strDateLogPath + ".log";
		String modelSavePath = baseModelPath + "dl4j_beer_review_strata.model";
		boolean loadPrevModel = true;
		String modelLoadPath = modelSavePath;

		EpochScoreTracker tracker = new EpochScoreTracker();
		tracker.setTargetLossScore(10.0);
		ArrayList<String> extraLogLines = new ArrayList<>();

		BeerReviewSequenceRecordReader reader = new BeerReviewSequenceRecordReader(dataPath, pathToBeerData, numExamplesPerEpoch);
		SequenceRecordReaderDataSetIterator iter = new SequenceRecordReaderDataSetIterator(reader, miniBatchSize, reader.getVocabSize(), 0, false);
		int nOut = iter.totalOutcomes();

		//Set up network configuration:
		MultiLayerConfiguration conf = new NeuralNetConfiguration.Builder()
			.optimizationAlgo(OptimizationAlgorithm.STOCHASTIC_GRADIENT_DESCENT).iterations(1)
			.learningRate(0.2)
			.rmsDecay(0.95)
			.seed(12345)
			.regularization(true)
			.l2(0.000001)
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
		
		if (loadPrevModel)
			System.out.println("Attemping to load existing model for continued training from local disk: " + modelLoadPath );
			try {
				net = loadModelFromLocalDisk(modelLoadPath);
			} catch(Exception e) {
				System.out.println("Failed!");
				loadPrevModel = false;
			}
		if (!loadPrevModel){
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
		
		//Do training, and then generate and print samples from network
		for( int i=0; i<numEpochs; i++ ){
		    if (!iter.hasNext())
			iter.reset();
		        System.out.println("Begin epoch " + i);
			start = System.currentTimeMillis();
			
			net.fit(iter);

			long end = System.currentTimeMillis();
			long epochSeconds = Math.abs(end - start) / 1000; // se
			long epochMin = epochSeconds / 60; // se

			totalTrainingTimeMS += Math.abs(end - start);


			totalExamplesAcrossEpochs += numExamplesPerEpoch;

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

			String[] samples = sampleBeerRatingFromNetwork(net, reader, rng, nCharactersToSample, 2);

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

		long totalTrainingTimeMinutesFinal = (totalTrainingTimeMS / 1000 / 60);

		System.out.println( "Total Training Time: " + totalTrainingTimeMinutesFinal + " minutes" );

		System.out.println( "Training First Loss Score: " + tracker.firstScore );
		System.out.println( "Training Average Loss Score: " + tracker.avgScore() );
		System.out.println( "Training Loss Score Improvement: " + tracker.scoreChangeOverWindow() );

		extraLogLines.add( "Mini-Batch Size: " + miniBatchSize );
		extraLogLines.add( "LSTM Layer Size: " + lstmLayerSize );
		extraLogLines.add( "Training Dataset: " + dataPath );

		extraLogLines.add( "Total Epochs: " + numEpochs );
		extraLogLines.add( "Training Time: " + totalTrainingTimeMinutesFinal );

		long avgTrainingTimePerEpochInMin = totalTrainingTimeMinutesFinal / numEpochs;

		extraLogLines.add( "Avg Training Time per Epoch: " + avgTrainingTimePerEpochInMin );

		extraLogLines.add( "Records in epoch: " + numExamplesPerEpoch );
		extraLogLines.add( "Records in dataset: " + reader.getCount() );
		extraLogLines.add( "Records Seen Across Epochs: " + totalExamplesAcrossEpochs );
		extraLogLines.add( "Training First Loss Score: " + tracker.firstScore );
		extraLogLines.add( "Training Average Loss Score: " + tracker.avgScore() );
		extraLogLines.add( "Training Total Loss Score Improvement: " + tracker.scoreChangeOverWindow() );
		extraLogLines.add( "Training Average Loss Score Improvement per Epoch: " + tracker.averageLossImprovementPerEpoch() );

		// tracker.computeProjectedEpochsRemainingToTargetLossScore()
		extraLogLines.add( "Targeted Loss Score: " + tracker.targetLossScore );

		double remainingEpochs = tracker.computeProjectedEpochsRemainingToTargetLossScore();

		extraLogLines.add( "Projected Remaining Epochs to Loss Target: " + remainingEpochs );

		double projectedRemainingTime = avgTrainingTimePerEpochInMin * remainingEpochs;

		extraLogLines.add( "Projected Remaining Minutes to Loss Target: " + projectedRemainingTime );

		Utils.writeLinesToLocalDisk(extraLogLinesPath, extraLogLines);

		System.out.println("\n\nExample complete");
		
	}

	private static String[] sampleBeerRatingFromNetwork(MultiLayerNetwork net, BeerReviewSequenceRecordReader iter,
														Random rng, int maxCharactersToSample,
														int styleIndex ){
		int numSamples = 5;
		int vocabSize = iter.getVocabSize();
		int numStyles = iter.getBeerStyleCount();
		INDArray context = Nd4j.zeros(numSamples, numStyles + 5);
		for( int sampleIndex = 0; sampleIndex < numSamples; sampleIndex++ ) {
			context.putScalar(new int[]{sampleIndex, styleIndex}, 1.0f);
			context.putScalar(new int[]{sampleIndex, numStyles + 0}, 0.0f);
			context.putScalar(new int[]{sampleIndex, numStyles + 1}, 0.0f);
			context.putScalar(new int[]{sampleIndex, numStyles + 2}, 0.0f);
			context.putScalar(new int[]{sampleIndex, numStyles + 3}, 0.0f);
			context.putScalar(new int[]{sampleIndex, numStyles + 4}, sampleIndex / 2.0f - 1);
		}

		int stopWordIndex = iter.convertCharacterToIndex(iter.STOPWORD);
		int[] sampledCharIndex = new int[numSamples];
		for (int i = 0; i < sampledCharIndex.length; i++) sampledCharIndex[i] = stopWordIndex;

		StringBuilder[] sb = new StringBuilder[numSamples];
		for (int i = 0; i < sb.length; i++) sb[i] = new StringBuilder();
		boolean[] continueBuilding = new boolean[numSamples];
		for (int i = 0; i < continueBuilding.length; i++) continueBuilding[i] = true;

		//Sample from network (and feed samples back into input) one character at a time (for all samples)
		//Sampling is done in parallel here

		net.rnnClearPreviousState();
		for( int i=0; i<maxCharactersToSample; i++ ){
			INDArray nextInput = Nd4j.zeros(numSamples, vocabSize + numStyles + 5);
			nextInput.get(NDArrayIndex.all(), NDArrayIndex.interval(vocabSize, vocabSize + numStyles + 5)).assign(context);
			for (int s=0; s < numSamples; s++)
				nextInput.putScalar(new int[]{ s, sampledCharIndex[s]}, 1.0f);
			INDArray output = net.rnnTimeStep( nextInput );
			for( int s=0; s<numSamples; s++ ){
				double[] outputProbDistribution = new double[vocabSize];
				for( int j=0; j<outputProbDistribution.length; j++ ) outputProbDistribution[j] = output.getDouble(s,j);
				sampledCharIndex[s] = sampleFromDistribution(outputProbDistribution,rng);
				nextInput.putScalar(new int[]{s,sampledCharIndex[s]}, 1.0f);		//Prepare next time step input
				if (sampledCharIndex[s] == iter.convertCharacterToIndex(iter.STOPWORD))
					continueBuilding[s] = false;
				if (continueBuilding[s])
					sb[s].append(iter.convertIndexToCharacter(sampledCharIndex[s]));	//Add sampled character to StringBuilder (human readable output)
			}

		}

		String[] out = new String[numSamples];
		for( int i=0; i<numSamples; i++ ) out[i] = sb[i].toString();
		return out;
	}

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
	
	public static MultiLayerNetwork loadModelFromLocalDisk(String path) throws IOException {
		
		File file = new File( path );
		
		MultiLayerNetwork network = ModelSerializer.restoreMultiLayerNetwork( file );
		
		return network;
	}
}
