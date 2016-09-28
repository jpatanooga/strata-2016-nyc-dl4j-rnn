package org.deeplearning4j.examples.rnn.beer;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.FileSystems;
import java.util.ArrayList;
import java.util.Date;
import java.util.Map;
import java.util.Random;

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
import org.deeplearning4j.optimize.api.IterationListener;
import org.deeplearning4j.optimize.listeners.CollectScoresIterationListener;
import org.deeplearning4j.optimize.listeners.ParamAndGradientIterationListener;
import org.deeplearning4j.optimize.listeners.PerformanceListener;
import org.deeplearning4j.optimize.listeners.ScoreIterationListener;
import org.deeplearning4j.ui.flow.FlowIterationListener;
import org.deeplearning4j.ui.weights.HistogramIterationListener;
import org.deeplearning4j.util.ModelSerializer;
import org.nd4j.linalg.api.ndarray.INDArray;
import org.nd4j.linalg.dataset.DataSet;
import org.nd4j.linalg.lossfunctions.LossFunctions.LossFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LSTMBeerReviewModelingExample {
    private static Logger log = LoggerFactory.getLogger(LSTMBeerReviewModelingExample.class);

	public static void main(String[] args) throws Exception {
        log.info("ARGS: " + args.length);
        final int LAGER = 2;
		int lstmLayerSize = Integer.parseInt(args[0]);        //Number of units in each GravesLSTM layer
		int miniBatchSize = Integer.parseInt(args[1]);	      //Size of mini batch to use when  training
		int maxExamplesPerEpoch = 0;                          //maximum # examples to train on (for debugging)
		int maxExampleLength = Integer.parseInt(args[2]);	  //If >0 we truncate longer sequences, exclude shorter sequences
		int tbpttLength = Integer.parseInt(args[3]);          //Truncated backprop through time, i.e., do parameter updates ever 50 characters
		int numEpochs = Integer.parseInt(args[4]);			  //Total number of training + sample generation epochs
		int nSamplesToGenerate = 5;                           //Number of samples to generate after each training epoch
		double temperature = Double.parseDouble(args[5]);
        boolean loadPrevModel = true;
        String generationInitialization = "~"; //Optional character initialization; a random character is used if null
		// Above is Used to 'prime' the LSTM with a character sequence to continue/complete.
		// Initialization characters must all be in CharacterIterator.getMinimalCharacterSet() by default
		int rngSeed = 12345;
        Random rng = new Random(rngSeed);

        String SEP = FileSystems.getDefault().getSeparator();
        String dataPath = System.getenv("BEER_REVIEW_PATH");
        File tempFile = new File(dataPath);
        assert(tempFile.exists() && !tempFile.isDirectory());
		String beerIdDataPath = dataPath + SEP + "beers_all.json";
		String reviewTrainingDataPath = dataPath + SEP + "reviews_top-train.json";
        String reviewTestDataPath = dataPath + SEP + "reviews_top-test.json";

		EpochScoreTracker tracker = new EpochScoreTracker();
		tracker.setTargetLossScore(10.0);
		ArrayList<String> extraLogLines = new ArrayList<>();

        char[] validCharacters = BeerReviewCharacterIterator.getDefaultCharacterSet();
        BeerReviewCharacterIterator trainData = new BeerReviewCharacterIterator(reviewTrainingDataPath, beerIdDataPath,
                                                    Charset.forName("UTF-8"), miniBatchSize, maxExampleLength,
                                                    maxExamplesPerEpoch, validCharacters, rng);
        BeerReviewCharacterIterator testData = new BeerReviewCharacterIterator(reviewTestDataPath, beerIdDataPath,
                                                    Charset.forName("UTF-8"), miniBatchSize, maxExampleLength,
                                                    maxExamplesPerEpoch, validCharacters, rng);
        BeerReviewCharacterIterator partTestData = new BeerReviewCharacterIterator(reviewTestDataPath, beerIdDataPath,
                                                    Charset.forName("UTF-8"), miniBatchSize, maxExampleLength,
                                                    miniBatchSize, validCharacters, rng);

        int everyNEpochs = 10; //trainData.totalExamples() / miniBatchSize / 50;
        String baseModelPath = System.getenv("MODEL_SAVE_PATH");
        ModelSaver saver = new ModelSaver(baseModelPath, everyNEpochs);
        String modelSavePath = saver.getModelSavePath();

        int totalExamples = trainData.totalExamples();
        if (maxExamplesPerEpoch <= 0 || maxExamplesPerEpoch > totalExamples)
            maxExamplesPerEpoch = totalExamples;
        int nIn = trainData.inputColumns();
		int nOut = trainData.totalOutcomes();

		//Set up network configuration:
		MultiLayerConfiguration conf = new NeuralNetConfiguration.Builder()
			.optimizationAlgo(OptimizationAlgorithm.STOCHASTIC_GRADIENT_DESCENT).iterations(1)
			.learningRate(0.1)
			.rmsDecay(0.95)
			.seed(rngSeed)
			.regularization(true)
			.l2(0.001)
            .weightInit(WeightInit.XAVIER)
            .updater(Updater.RMSPROP)
			.list()
			.layer(0, new GravesLSTM.Builder()
                            .nIn(nIn)
                            .nOut(lstmLayerSize)
					.activation("tanh").build())
			.layer(1, new GravesLSTM.Builder()
                            .nOut(lstmLayerSize)
					.activation("tanh").build())
			.layer(2, new RnnOutputLayer.Builder(LossFunction.MCXENT)
                            .activation("softmax")
                            .nOut(nOut)
                            .build())
            .backpropType(BackpropType.TruncatedBPTT)
            .tBPTTForwardLength(tbpttLength)
            .tBPTTBackwardLength(tbpttLength)
			.pretrain(false)
            .backprop(true)
			.build();
		MultiLayerNetwork net = new MultiLayerNetwork(conf);
		INDArray params = null;
		if (loadPrevModel) {
			log.info("Attempting to continue training from " + modelSavePath);
            try {
				MultiLayerNetwork oldNet = ModelSerializer.restoreMultiLayerNetwork(modelSavePath);
				params = oldNet.params();
				log.info("Success!");
            } catch (Exception e) {
                log.info("Failed to load model from " + modelSavePath);
                loadPrevModel = false;
				log.info("Starting with a new model...");
            }
		}
		net.init(params, false);

		ArrayList<IterationListener> listeners = new ArrayList<>();
        listeners.add(saver);
//		listeners.add(new HistogramIterationListener(1));
//		listeners.add(new FlowIterationListener(1));
		listeners.add(new CollectScoresIterationListener(10));
		listeners.add(new PerformanceListener(everyNEpochs, true));
//		listeners.add(new ScoreIterationListener(1));
        listeners.add(new HeldoutScoreIterationListener(partTestData, 2*miniBatchSize, 2));
		File gradientFile = new File(baseModelPath + FileSystems.getDefault().getSeparator() + "gradients.tsv");
		listeners.add(new ParamAndGradientIterationListener(everyNEpochs, true, false, false, true, false, true, true,
				gradientFile, "\t"));
        listeners.add(new SampleGeneratorListener(net, trainData, rng, temperature, maxExampleLength, LAGER, everyNEpochs));
		net.setListeners(listeners);

		//Print the  number of parameters in the network (and for each layer)
		Layer[] layers = net.getLayers();
		int totalNumParams = 0;
		for(int i=0; i<layers.length; i++){
			int nParams = layers[i].numParams();
			log.info("Number of parameters in layer " + i + ": " + nParams);
			totalNumParams += nParams;
		}
		log.info("Total number of network parameters: " + totalNumParams);
		
		long totalExamplesAcrossEpochs = 0;
		long start;
		long totalTrainingTimeMS = 0;

		log.info("----- Generating Initial Lager Beer Review Sample -----");
		String[] initialSample = SampleGeneratorListener.sampleBeerRatingFromNetwork(net, trainData, rng, temperature,
									maxExampleLength > 0 ? maxExampleLength : 1000, 1, LAGER);
		log.info("SAMPLE 00: " + initialSample[0]);

		//Do training, and then generate and print samples from network
        String evalStr = "Test set evaluation at epoch %d: Accuracy = %.2f, F1 = %.2f";
		for(int i=0; i<numEpochs; i++){
			log.info("EPOCH " + i);
			start = System.currentTimeMillis();
			net.fit(trainData);
            long end = System.currentTimeMillis();
            long epochSeconds = Math.abs(end - start) / 1000; // se
            long epochMin = epochSeconds / 60; // se
            
            totalTrainingTimeMS += Math.abs(end - start);
			totalExamplesAcrossEpochs += maxExamplesPerEpoch;
			
			log.info("--------------------");
			log.info("Completed epoch " + i);
			
			log.info("Epoch Loss Score: " + net.score());
			tracker.addScore(net.score());
			
			log.info("Time for Epoch Training " + Math.abs(end - start) + " ms, (" + epochSeconds + " seconds) (" + (epochMin) + " minutes)");
			log.info("Total Training Time so Far: " + (totalTrainingTimeMS / 1000 / 60) + " minutes");

            ModelSaver.saveModel(net, saver.getModelSavePath(), i);

            log.info("Evaluate model....");
//            int ct = testData.totalExamples();
            double cost = 0;
            double count = 0;
            while(testData.hasNext()) {
                DataSet minibatch = testData.next();
                cost += net.scoreExamples(testData, false).sumNumber().doubleValue();
                count += minibatch.getLabelsMaskArray().sumNumber().doubleValue();
            }
            log.info(String.format("Epoch %4d test set average cost: %.4f", i, cost / count));
//            testData.reset();
//            Evaluation evaluation = net.evaluate(testData);;
//            log.info(String.format(evalStr, i, ));
//            log.info(String.format(evalStr, i, evaluation.accuracy(), evaluation.f1()));
            log.info("****************Example finished********************");

//            float rating_overall = 4.0f;
//            float rating_taste = 5.0f;
//            float rating_appearance = 4.0f;
//            float rating_palate = 5.0f;
//            float rating_aroma = 5.0f;
//            int styleIndex = 2; // Lager
//			  String[] samples = sampleBeerRatingFromNetwork(generationInitialization, net, iter, rng, nCharactersToSample, nSamplesToGenerate, rating_overall, rating_taste, rating_appearance, rating_palate, rating_aroma, styleIndex);
//			  for(int j=0; j<samples.length; j++){
//				  log.info("----- Generating Lager Beer Review Sample [" + j + "] -----");
//				  log.info(samples[j]);
//			  }
            trainData.reset();
            testData.reset();

            for (String s : SampleGeneratorListener.sampleBeerRatingFromNetwork(net, trainData, rng, temperature,
																				maxExampleLength, 5, 2))
                log.info("SAMPLE: " + s);
            int a = 1;
		}
		
		log.info("Training Final Report: ");
		long totalTrainingTimeMinutesFinal = (totalTrainingTimeMS / 1000 / 60);
		log.info("Total Training Time: " + totalTrainingTimeMinutesFinal + " minutes");
		log.info("Training First Loss Score: " + tracker.firstScore);
		log.info("Training Average Loss Score: " + tracker.avgScore());
		log.info("Training Loss Score Improvement: " + tracker.scoreChangeOverWindow());
		
		extraLogLines.add("Mini-Batch Size: " + miniBatchSize);
		extraLogLines.add("LSTM Layer Size: " + lstmLayerSize);
		extraLogLines.add("Training Dataset: " + dataPath);
		extraLogLines.add("Total Epochs: " + numEpochs);
		extraLogLines.add("Training Time: " + totalTrainingTimeMinutesFinal);
        long avgTrainingTimePerEpochInMin = totalTrainingTimeMinutesFinal / numEpochs;
		extraLogLines.add("Avg Training Time per Epoch: " + avgTrainingTimePerEpochInMin);
		extraLogLines.add("Records in epoch: " + maxExamplesPerEpoch);
		extraLogLines.add("Records in dataset: " + trainData.numExamples());
		extraLogLines.add("Records Seen Across Epochs: " + totalExamplesAcrossEpochs);
		extraLogLines.add("Training First Loss Score: " + tracker.firstScore);
		extraLogLines.add("Training Average Loss Score: " + tracker.avgScore());
		extraLogLines.add("Training Total Loss Score Improvement: " + tracker.scoreChangeOverWindow());
		extraLogLines.add("Training Average Loss Score Improvement per Epoch: " + tracker.averageLossImprovementPerEpoch());
		extraLogLines.add("Targeted Loss Score: " + tracker.targetLossScore);
        double remainingEpochs = tracker.computeProjectedEpochsRemainingToTargetLossScore();
        extraLogLines.add("Projected Remaining Epochs to Loss Target: " + remainingEpochs);
		extraLogLines.add("Projected Remaining Minutes to Loss Target: " + avgTrainingTimePerEpochInMin * remainingEpochs);
		Utils.writeLinesToLocalDisk(saver.getExtraLogLinesPath(), extraLogLines);

		log.info("Example complete");
	}
//
//	private static String[] sampleBeerRatingFromNetwork(MultiLayerNetwork net, BeerReviewCharacterIterator iter,
//                                                        Random rng, int charactersToSample, int numSamples,
//			                                            float[] rating_overall, float[] rating_taste, float[] rating_appearance,
//                                                        float[] rating_palate, float[] rating_aroma, int styleIndex){
//
//        String initialization = "the";
//
//		//Create input for initialization
//		INDArray initializationInput = Nd4j.zeros(numSamples, iter.inputColumns(), initialization.length());
//		char[] init = initialization.toCharArray();
//
//		// for each timestep we want to generate a character
//		for(int charTimestep = 0; charTimestep < init.length; charTimestep++){
//
//			// get the column index for the current character
//			int col_idx = iter.convertCharacterToIndex(init[ charTimestep ]);
//
//			// for each sample
//			for(int sampleIndex = 0; sampleIndex < numSamples; sampleIndex++){
//
//				initializationInput.putScalar(new int[]{ sampleIndex, col_idx, charTimestep }, 1.0f);
//
//				// TODO: here is where we need to add in ratings for generation hints
//
//				int staticColumnBaseOffset = iter.numCharacterColumns();
//
//				initializationInput.putScalar(new int[]{ sampleIndex, staticColumnBaseOffset, charTimestep }, rating_appearance);
//				initializationInput.putScalar(new int[]{ sampleIndex, staticColumnBaseOffset + 1, charTimestep }, rating_aroma);
//				initializationInput.putScalar(new int[]{ sampleIndex, staticColumnBaseOffset + 2, charTimestep }, rating_overall);
//				initializationInput.putScalar(new int[]{ sampleIndex, staticColumnBaseOffset + 3, charTimestep }, rating_palate);
//				initializationInput.putScalar(new int[]{ sampleIndex, staticColumnBaseOffset + 4, charTimestep }, rating_taste);
//
//
//				// SETUP STYLE INDEX
//				int styleColumnBaseOffset = staticColumnBaseOffset + 5;
//				int styleIndexColumn = styleColumnBaseOffset + styleIndex; // add the base to the index
//
//				//log.info("style index base: " + styleColumnBaseOffset + ", offset: " + styleIndexColumn);
//
//				initializationInput.putScalar(new int[]{ sampleIndex, styleIndexColumn, charTimestep }, 1.0);
//
//
//			}
//
//		}
//
//
//
//
//
//		// TODO: end ratings hints in input array
//
//
//		StringBuilder[] sb = new StringBuilder[numSamples];
//
//		for (int i = 0; i < numSamples; i++) {
//
//			sb[ i ] = new StringBuilder(initialization);
//
//		}
//
//		//Sample from network (and feed samples back into input) one character at a time (for all samples)
//		//Sampling is done in parallel here
//		net.rnnClearPreviousState();
//		INDArray output = net.rnnTimeStep(initializationInput);
//		output = output.tensorAlongDimension(output.size(2)-1,1,0);	//Gets the last time step output
//
//		for(int i=0; i<charactersToSample; i++){
//			//Set up next input (single time step) by sampling from previous output
//			INDArray nextInput = Nd4j.zeros(numSamples,iter.inputColumns());
//			//Output is a probability distribution. Sample from this for each example we want to generate, and add it to the new input
//			for(int s=0; s<numSamples; s++){
//				double[] outputProbDistribution = new double[iter.totalOutcomes()];
//				for(int j=0; j<outputProbDistribution.length; j++) outputProbDistribution[j] = output.getDouble(s,j);
//				int sampledCharacterIdx = sampleFromDistribution(outputProbDistribution,rng);
//
//				nextInput.putScalar(new int[]{s,sampledCharacterIdx}, 1.0f);		//Prepare next time step input
//				sb[s].append(iter.convertIndexToCharacter(sampledCharacterIdx));	//Add sampled character to StringBuilder (human readable output)
//			}
//
//			output = net.rnnTimeStep(nextInput);	//Do one time step of forward pass
//		}
//
//		String[] out = new String[numSamples];
//		for(int i=0; i<numSamples; i++) out[i] = sb[i].toString();
//		return out;
//	}
//
//
//	/** Generate a sample from the network, given an (optional, possibly null) initialization. Initialization
//	 * can be used to 'prime' the RNN with a sequence you want to extend/continue.<br>
//	 * Note that the initalization is used for all samples
//	 * @param initialization String, may be null. If null, select a random character as initialization for all samples
//	 * @param charactersToSample Number of characters to sample from network (excluding initialization)
//	 * @param net MultiLayerNetwork with one or more GravesLSTM/RNN layers and a softmax output layer
//	 * @param iter CharacterIterator. Used for going from indexes back to characters
//	 */
//	private static String[] sampleCharactersFromNetwork(String initialization, MultiLayerNetwork net,
//			BeerReviewCharacterIterator iter, Random rng, int charactersToSample, int numSamples){
//		//Set up initialization. If no initialization: use a random character
//		if(initialization == null){
//			initialization = String.valueOf(iter.getRandomCharacter());
//		}
//
//		//Create input for initialization
//		INDArray initializationInput = Nd4j.zeros(numSamples, iter.inputColumns(), initialization.length());
//		char[] init = initialization.toCharArray();
//		for(int i=0; i<init.length; i++){
//			int idx = iter.convertCharacterToIndex(init[i]);
//			for(int j=0; j<numSamples; j++){
//				initializationInput.putScalar(new int[]{j,idx,i}, 1.0f);
//			}
//		}
//
//		StringBuilder[] sb = new StringBuilder[numSamples];
//		for(int i=0; i<numSamples; i++) sb[i] = new StringBuilder(initialization);
//
//		//Sample from network (and feed samples back into input) one character at a time (for all samples)
//		//Sampling is done in parallel here
//		net.rnnClearPreviousState();
//		INDArray output = net.rnnTimeStep(initializationInput);
//		output = output.tensorAlongDimension(output.size(2)-1,1,0);	//Gets the last time step output
//
//		for(int i=0; i<charactersToSample; i++){
//			//Set up next input (single time step) by sampling from previous output
//			INDArray nextInput = Nd4j.zeros(numSamples,iter.inputColumns());
//			//Output is a probability distribution. Sample from this for each example we want to generate, and add it to the new input
//			for(int s=0; s<numSamples; s++){
//				double[] outputProbDistribution = new double[iter.totalOutcomes()];
//				for(int j=0; j<outputProbDistribution.length; j++) outputProbDistribution[j] = output.getDouble(s,j);
//				int sampledCharacterIdx = sampleFromDistribution(outputProbDistribution,rng);
//
//				nextInput.putScalar(new int[]{s,sampledCharacterIdx}, 1.0f);		//Prepare next time step input
//				sb[s].append(iter.convertIndexToCharacter(sampledCharacterIdx));	//Add sampled character to StringBuilder (human readable output)
//			}
//
//			output = net.rnnTimeStep(nextInput);	//Do one time step of forward pass
//		}
//
//		String[] out = new String[numSamples];
//		for(int i=0; i<numSamples; i++) out[i] = sb[i].toString();
//		return out;
//	}
//
//	/** Given a probability distribution over discrete classes, sample from the distribution
//	 * and return the generated class index.
//	 * @param distribution Probability distribution over classes. Must sum to 1.0
//	 */
//	private static int sampleFromDistribution(double[] distribution, Random rng){
//		double d = rng.nextDouble();
//		double sum = 0.0;
//		for(int i=0; i<distribution.length; i++){
//			sum += distribution[i];
//			if(d <= sum) return i;
//		}
//		//Should never happen if distribution is a valid probability distribution
//		throw new IllegalArgumentException("Distribution is invalid? d="+d+", sum="+sum);
//	}
}
