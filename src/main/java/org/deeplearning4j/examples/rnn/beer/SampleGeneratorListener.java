package org.deeplearning4j.examples.rnn.beer;

import org.deeplearning4j.examples.rnn.beer.BeerReviewCharacterIterator;
import org.deeplearning4j.nn.api.Model;
import org.deeplearning4j.nn.multilayer.MultiLayerNetwork;
import org.deeplearning4j.optimize.api.IterationListener;
import org.nd4j.linalg.api.ndarray.INDArray;
import org.nd4j.linalg.factory.Nd4j;
import org.nd4j.linalg.indexing.NDArrayIndex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;

/**
 * Created by davekale on 9/20/16.
 */
public class SampleGeneratorListener implements IterationListener {

    private static final Logger log = LoggerFactory.getLogger(SampleGeneratorListener.class);
    private int printIterations;
    private long iterCount = 0;

    private int styleIndex;
    private int maxCharactersToSample > 0 ? maxCharactersToSample : 1000;
    private Random rng;
    private BeerReviewCharacterIterator reader;
    private MultiLayerNetwork net;
    private boolean invoked = false;

    public SampleGeneratorListener(MultiLayerNetwork net, BeerReviewCharacterIterator reader, Random rng,
                                   int maxCharactersToSample, int styleIndex, int printIterations) {
        this.net = net;
        this.reader = reader;
        this.rng = rng;
        this.maxCharactersToSample = maxCharactersToSample;
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
            String[] samples = sampleBeerRatingFromNetwork(net, reader, rng, maxCharactersToSample, 1, styleIndex);

            log.info("----- Generating Lager Beer Review Samples -----");
            for (int j = 0; j < samples.length; j++) {
                log.info("SAMPLE " + j + ": " + samples[j]);
            }
        }
        iterCount++;
    }

    public static String[] sampleBeerRatingFromNetwork(MultiLayerNetwork net, BeerReviewCharacterIterator iter,
                                                        Random rng, int maxCharactersToSample, int numSamples,
                                                        int styleIndex) {
        numSamples = numSamples > 5 ? 5 : numSamples;
        int staticColumnBaseOffset = iter.numCharacterColumns();
        int styleColumnBaseOffset = staticColumnBaseOffset + iter.numRatings();
        int styleIndexColumn = styleColumnBaseOffset + styleIndex;

        net.clearLayerMaskArrays();
        net.rnnClearPreviousState();

        INDArray input = Nd4j.zeros(new int[]{numSamples, iter.inputColumns()});
        for (int s = 0; s < numSamples; s++) {
            input.putScalar(new int[]{s, staticColumnBaseOffset    }, 1); //5);
            input.putScalar(new int[]{s, staticColumnBaseOffset + 1}, 1); //5);
            input.putScalar(new int[]{s, staticColumnBaseOffset + 2}, s/2.0 - 1); //s + 1);
            input.putScalar(new int[]{s, staticColumnBaseOffset + 3}, 1); //5);
            input.putScalar(new int[]{s, staticColumnBaseOffset + 4}, 1); //5);
            input.putScalar(new int[]{s, styleIndexColumn}, 1.0);
        }

        StringBuilder[] sb = new StringBuilder[numSamples];
        boolean[] continueBuilding = new boolean[numSamples];
        for (int s = 0; s < numSamples; s++) {
            sb[s] = new StringBuilder();
            continueBuilding[s] = true;
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
                prevCharIdx[s] = currCharIdx[s];
                currCharIdx[s] = sampleFromDistribution(outputProbDistribution, rng);
//                if (currCharIdx[s] == iter.STOPWORD)
//                    continueBuilding[s] = false;
                if (continueBuilding[s])
                    sb[s].append(iter.convertIndexToCharacter(currCharIdx[s]));
            }
        }

        String[] out = new String[numSamples];
        for (int s = 0; s < numSamples; s++)
            out[s] = sb[s].toString();
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
}
