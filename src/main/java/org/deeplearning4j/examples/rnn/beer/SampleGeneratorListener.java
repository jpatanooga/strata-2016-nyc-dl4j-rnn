package org.deeplearning4j.examples.rnn.beer;

import org.apache.spark.sql.catalyst.plans.logical.Sample;
import org.deeplearning4j.nn.api.Layer;
import org.deeplearning4j.nn.api.Model;
import org.deeplearning4j.nn.multilayer.MultiLayerNetwork;
import org.deeplearning4j.optimize.api.IterationListener;
import org.deeplearning4j.util.ModelSerializer;
import org.nd4j.linalg.api.ndarray.INDArray;
import org.nd4j.linalg.factory.Nd4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.charset.Charset;
import java.nio.file.FileSystems;
import java.util.Random;
import java.lang.Math;

/**
 * Created by davekale on 9/20/16.
 */
public class SampleGeneratorListener implements IterationListener {

    private static final Logger log = LoggerFactory.getLogger(SampleGeneratorListener.class);
    private int printIterations;
    private long iterCount = 0;

    private int styleIndex;
    private int maxCharactersToSample;
    private Random rng;
    private double temperature = 0;
    private BeerReviewCharacterIterator reader;
    private MultiLayerNetwork net;
    private boolean invoked = false;

    public SampleGeneratorListener(MultiLayerNetwork net, BeerReviewCharacterIterator reader, Random rng,
                                   int maxCharactersToSample, int styleIndex, int printIterations) {
        this(net, reader, rng, 0, maxCharactersToSample, styleIndex, printIterations);
    }

    public SampleGeneratorListener(MultiLayerNetwork net, BeerReviewCharacterIterator reader, Random rng,
                                   double temperature, int maxCharactersToSample, int styleIndex, int printIterations) {
        this.net = net;
        this.reader = reader;
        this.rng = rng;
        this.temperature = temperature;
        this.maxCharactersToSample = maxCharactersToSample > 0 ? maxCharactersToSample : 1000;
        this.styleIndex = styleIndex;
        this.printIterations = printIterations;
    }

    @Override
    public boolean invoked() {
        return invoked;
    }

    @Override
    public void invoke() {
        invoked = true;
    }

    @Override
    public void iterationDone(Model model, int i) {
        if(printIterations <= 0)
            printIterations = 1;
        if (iterCount % printIterations == 0) {
            invoke();
            String[] samples = sampleBeerRatingFromNetwork(net, reader, rng, temperature, maxCharactersToSample, 1, styleIndex);

            System.out.println("----- Generating Lager Beer Review Samples -----");
            for (int j = 0; j < samples.length; j++) {
                System.out.println("SAMPLE " + j + ": " + samples[j]);
            }
        }
        iterCount++;
    }

    public static String[] sampleBeerRatingFromNetwork(MultiLayerNetwork net, BeerReviewCharacterIterator iter, Random rng,
                                                       double temperature, int maxCharactersToSample, int numSamples,
                                                       int styleIndex) {
        return sampleBeerRatingFromNetwork(net, iter, rng, temperature, maxCharactersToSample, numSamples, styleIndex,
                                            5, 5, 5, 5, 5);
    }

    public static String[] sampleBeerRatingFromNetwork(MultiLayerNetwork net, BeerReviewCharacterIterator iter,
                                                       Random rng, double temperature, int maxCharactersToSample,
                                                       int numSamples, int styleIndex, int overallRating, int tasteRating,
                                                       int appearanceRating, int palateRating, int aromaRating) {
        numSamples = numSamples > 5 ? 5 : numSamples;
        int staticColumnBaseOffset = iter.numCharacterColumns();
        int styleColumnBaseOffset = staticColumnBaseOffset + iter.numRatings();
        int styleIndexColumn = styleColumnBaseOffset + styleIndex;

        net.clearLayerMaskArrays();
        net.rnnClearPreviousState();

        INDArray input = Nd4j.zeros(new int[]{numSamples, iter.inputColumns()});
        for (int s = 0; s < numSamples; s++) {
            input.putScalar(new int[]{s, staticColumnBaseOffset    }, (tasteRating-1)/2.0-1); //5);
            input.putScalar(new int[]{s, staticColumnBaseOffset + 1}, (appearanceRating-1)/2.0-1); //5);
            input.putScalar(new int[]{s, staticColumnBaseOffset + 2}, (overallRating-1)/2.0-1); //s + 1);
            input.putScalar(new int[]{s, staticColumnBaseOffset + 3}, (palateRating-1)/2.0-1); //5);
            input.putScalar(new int[]{s, staticColumnBaseOffset + 4}, (aromaRating-1)/2.0-1); //5);
            input.putScalar(new int[]{s, styleIndexColumn}, 1.0);
        }

        StringBuilder[] sb = new StringBuilder[numSamples];
        boolean[] continueBuilding = new boolean[numSamples];
        for (int s = 0; s < numSamples; s++) {
            sb[s] = new StringBuilder();
            continueBuilding[s] = true;
        }

        boolean stopAutomatically = false;
        if (maxCharactersToSample <= 0) {
            maxCharactersToSample = 5000;
            stopAutomatically = true;
        }

        int[] prevCharIdx = new int[numSamples];
        int[] currCharIdx = new int[numSamples];
        for (int s = 0; s < numSamples; s++) {
            prevCharIdx[s] = iter.convertCharacterToIndex(iter.STOPWORD);
            currCharIdx[s] = iter.convertCharacterToIndex(iter.STOPWORD);
        }

        for (int i = 0; i < maxCharactersToSample; i++) {
            for (int s = 0; s < numSamples; s++) {
                input.putScalar(new int[]{s, prevCharIdx[s]}, 0.0);
                input.putScalar(new int[]{s, currCharIdx[s]}, 1.0);
            }
            INDArray output = net.rnnTimeStep(input);
            for (int s = 0; s < numSamples; s++) {
                double[] outputProbDistribution = new double[iter.numCharacterColumns()];
                for (int j = 0; j < outputProbDistribution.length; j++)
                    outputProbDistribution[j] = output.getDouble(s, j);
                if (temperature > 0) {
                    double outputSum = 0;
                    for (int j = 0; j < outputProbDistribution.length; j++) {
                        outputProbDistribution[j] = Math.exp(outputProbDistribution[j] * temperature);
                        outputSum += outputProbDistribution[j];
                    }
                    for (int j = 0; j < outputProbDistribution.length; j++)
                        outputProbDistribution[j] /= outputSum;
                }
                prevCharIdx[s] = currCharIdx[s];
                int newCharIdx = iter.STOPWORD;
                if (!stopAutomatically)
                    while (newCharIdx == iter.STOPWORD)
                        newCharIdx = sampleFromDistribution(outputProbDistribution, rng);
                if (stopAutomatically && newCharIdx == iter.STOPWORD)
                    continueBuilding[s] = false;
                currCharIdx[s] = newCharIdx;
                if (continueBuilding[s])
                    sb[s].append(iter.convertIndexToCharacter(currCharIdx[s]));
            }
        }

        String[] out = new String[numSamples];
        for (int s = 0; s < numSamples; s++)
            out[s] = sb[s].toString();
        return out;
    }

    private static int sampleFromDistribution( double[] distribution, Random rng){
        double d = rng.nextDouble();
        double sum = 0.0;
        for( int i=0; i<distribution.length; i++ ){
            sum += distribution[i];
            if( d <= sum ) return i;
        }
        //Should never happen if distribution is a valid probability distribution
        throw new IllegalArgumentException("Distribution is invalid? d="+d+", sum="+sum);
    }

    public static void main(String[] args) throws Exception {
        System.out.println("ARGS: " + args.length);
//        int rngSeed = 74756840;
        Random rng = new Random();
        double temperature = 0;

        String SEP = FileSystems.getDefault().getSeparator();
        String dataPath = System.getenv("BEER_REVIEW_PATH");
        File tempFile = new File(dataPath);
        assert(tempFile.exists() && !tempFile.isDirectory());
        String beerIdDataPath = dataPath + SEP + "beers_all.json";
        String reviewTrainingDataPath = dataPath + SEP + "reviews_top-train.json";

        char[] validCharacters = BeerReviewCharacterIterator.getDefaultCharacterSet();
        BeerReviewCharacterIterator trainData = new BeerReviewCharacterIterator(reviewTrainingDataPath, beerIdDataPath,
                                                            Charset.forName("UTF-8"), 128, 0, 0, validCharacters, rng);

        String baseModelPath = System.getenv("MODEL_SAVE_PATH");
        ModelSaver saver = new ModelSaver(baseModelPath, 1);
        String modelSavePath = saver.getModelSavePath();
        System.out.println("Attempting to continue training from " + modelSavePath);
        MultiLayerNetwork net = null;
        try {
            net = ModelSerializer.restoreMultiLayerNetwork(modelSavePath);
        } catch (Exception e) {
            System.out.println("Failed to load model from " + modelSavePath);
            System.exit(1);
        }

        if (net != null) {
            //Print the  number of parameters in the network (and for each layer)
            Layer[] layers = net.getLayers();
            int totalNumParams = 0;
            for (int i = 0; i < layers.length; i++) {
                int nParams = layers[i].numParams();
                System.out.println("Number of parameters in layer " + i + ": " + nParams);
                totalNumParams += nParams;
            }
            System.out.println("Total number of network parameters: " + totalNumParams);

            int nbSamples = 2;
            for (int styleIndex = 2; styleIndex <= 2; styleIndex++) {
                for (int rating = 1; rating <= 5; rating++) {
                    System.out.println("Generating " + nbSamples + " of a " + rating + " star " + styleIndex);
                    System.out.println("--------------------");
                    String[] reviews = sampleBeerRatingFromNetwork(net, trainData, rng, temperature, 0, nbSamples / 2,
                            rating, rating, rating, rating, rating, rating);
                    int i = 0;
                    for (String review : reviews) {
                        System.out.println("SAMPLE " + ++i + ":" + review);
                    }
                    reviews = sampleBeerRatingFromNetwork(net, trainData, rng, temperature, 600, nbSamples / 2, rating,
                            rating, rating, rating, rating, rating);
                    for (String review : reviews) {
                        System.out.println("SAMPLE " + ++i + ":" + review);
                    }
                    System.out.println("************************************\n");
                }
                System.out.println("************************************");
            }
        }
    }
}
