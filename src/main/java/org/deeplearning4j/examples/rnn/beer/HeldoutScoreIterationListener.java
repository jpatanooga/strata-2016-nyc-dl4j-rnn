package org.deeplearning4j.examples.rnn.beer;

import org.deeplearning4j.nn.api.Model;
import org.deeplearning4j.nn.multilayer.MultiLayerNetwork;
import org.deeplearning4j.optimize.api.IterationListener;
import org.nd4j.linalg.api.ndarray.INDArray;
import org.nd4j.linalg.dataset.DataSet;
import org.nd4j.linalg.dataset.api.iterator.DataSetIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by davekale on 9/21/16.
 */
public class HeldoutScoreIterationListener implements IterationListener {
    private int miniBatchSize;
    private int printIterations;
    private DataSetIterator iter;
    private boolean invoked = false;
    private int iterCount = 0;

    private static Logger log = LoggerFactory.getLogger(HeldoutScoreIterationListener.class);
    
    public HeldoutScoreIterationListener(DataSetIterator iter, int miniBatchSize, int printIterations) {
        this.iter = iter;
        this.miniBatchSize = miniBatchSize;
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
        if (printIterations <= 0)
            printIterations = 1;
        if (iterCount % printIterations == 0) {
            iter.reset();
            double cost = 0;
            double count = 0;
            while(iter.hasNext()) {
                DataSet minibatch = iter.next(miniBatchSize);
                cost += ((MultiLayerNetwork)model).scoreExamples(minibatch, false).sumNumber().doubleValue();
                count += minibatch.getLabelsMaskArray().sumNumber().doubleValue();
            }
            log.info(String.format("Iteration %5d test set score: %.4f", iterCount, cost/count));
        }
        iterCount++;
    }
}
