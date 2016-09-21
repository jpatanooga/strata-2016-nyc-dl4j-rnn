package org.deeplearning4j.examples.rnn.beer.dev;

import org.deeplearning4j.examples.rnn.beer.schema.json.BeerReviewSequenceRecordReader;
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
    private int maxCharactersToSample;
    private Random rng;
    private BeerReviewSequenceRecordReader reader;
    private MultiLayerNetwork net;
    private boolean invoked = false;

    public SampleGeneratorListener(MultiLayerNetwork net, BeerReviewSequenceRecordReader reader, Random rng,
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
            String[] samples = sampleBeerRatingFromNetwork(net, reader, rng, maxCharactersToSample, styleIndex);

            log.info("----- Generating Lager Beer Review Samples -----");
            for (int j = 0; j < samples.length; j++) {
                log.info("SAMPLE " + j + ": " + samples[j]);
            }
        }
        iterCount++;
    }

    private static String[] sampleBeerRatingFromNetwork(MultiLayerNetwork net, BeerReviewSequenceRecordReader iter,
                                                        Random rng, int maxCharactersToSample, int styleIndex ){
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


        net.clearLayerMaskArrays();
        net.rnnClearPreviousState();
        for( int i=0; i<maxCharactersToSample; i++ ){
            INDArray nextInput = Nd4j.zeros(numSamples, vocabSize + numStyles + 5);
            nextInput.get(NDArrayIndex.all(), NDArrayIndex.interval(vocabSize, vocabSize + numStyles + 5)).assign(context);
            for (int s=0; s < numSamples; s++)
                nextInput.putScalar(new int[]{s, sampledCharIndex[s]}, 1.0f);
            INDArray output = net.rnnTimeStep( nextInput );
            for( int s=0; s<numSamples; s++ ){
                double[] outputProbDistribution = new double[vocabSize];
                for( int j=0; j<outputProbDistribution.length; j++ ) outputProbDistribution[j] = output.getDouble(s,j);
                sampledCharIndex[s] = sampleFromDistribution(outputProbDistribution,rng);
                nextInput.putScalar(new int[]{s, sampledCharIndex[s]}, 1.0f);		//Prepare next time step input
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
}
