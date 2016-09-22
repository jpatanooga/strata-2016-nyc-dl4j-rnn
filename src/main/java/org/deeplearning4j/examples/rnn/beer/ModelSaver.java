package org.deeplearning4j.examples.rnn.beer;

import org.deeplearning4j.nn.api.Model;
import org.deeplearning4j.nn.multilayer.MultiLayerNetwork;
import org.deeplearning4j.optimize.api.IterationListener;
import org.deeplearning4j.util.ModelSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by davekale on 9/21/16.
 */
public class ModelSaver implements IterationListener {

    private static final Logger log = LoggerFactory.getLogger(ModelSaver.class);

    private boolean invoked = false;
    private int printIterations = 0;
    private int iterCount = 0;
    private SimpleDateFormat sdf;
    private String strDatePath;
    private String modelSavePath;
    private String extraLogLinesPath;

    public ModelSaver(String baseModelPath, int printIterations) {
        String SEP = FileSystems.getDefault().getSeparator();
        sdf = new SimpleDateFormat("yyyy-MM-dd_HH_mm_ss");
        File logDirectory = new File(baseModelPath + SEP + "logs");
        if (!logDirectory.exists() || !logDirectory.isDirectory()) {
            logDirectory.mkdirs();
            log.info("Creating log directory: " + logDirectory.toString());
        }
        strDatePath = sdf.format(new Date());
        modelSavePath = baseModelPath + SEP + "dl4j_beer_review_strata.model";
        extraLogLinesPath = baseModelPath + SEP + "logs" + SEP + "dl4j_beer_review_" + strDatePath + ".log";
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
            saveModel((MultiLayerNetwork)model, this.modelSavePath);
        }
        iterCount++;
    }

    public String getModelSavePath() { return modelSavePath; }

    public String getExtraLogLinesPath() { return extraLogLinesPath; }

    static public void saveModel(MultiLayerNetwork net, String modelSavePath) {
        saveModel(net, modelSavePath, -1);
    }

    static public void saveModel(MultiLayerNetwork net, String modelSavePath, int epoch) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd_HH_mm_ss");
        try {
            String strDate = sdf.format(new Date());
            String fn = "dl4j_rnn_" + strDate;
            if (epoch >= 0)
                fn += String.format("epoch%04d", epoch);
            File modelOutFile = File.createTempFile(fn, ".model");
            ModelSerializer.writeModel(net, modelOutFile, true);
            log.info("Saved model to temp file" + modelOutFile.getAbsolutePath());
        } catch (IOException e) {
            log.info("Could not save temporary model file");
        }

        try {
            File modelOutFile = new File(modelSavePath);
            ModelSerializer.writeModel(net, modelOutFile, true);
            log.info("Model checkpoint saved to " + modelOutFile.getAbsolutePath());
        } catch (IOException e) {
            log.info("Could not save checkpoint file");
        }
    }
}
