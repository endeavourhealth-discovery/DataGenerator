package org.endeavourhealth.cegdatabasefilesender.feedback;

import org.apache.commons.io.FileUtils;
import org.endeavourhealth.cegdatabasefilesender.feedback.bean.FeedbackHolder;
import org.endeavourhealth.cegdatabasefilesender.feedback.bean.FileResult;
import org.endeavourhealth.cegdatabasefilesender.feedback.bean.Result;
import org.endeavourhealth.scheduler.json.SubscriberFileSenderDefinition.SubscriberFileSenderConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

public class FeedbackSlurper implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(FeedbackSlurper.class);
    private final SubscriberFileSenderConfig config;
    private final SftpFeedback sftpFeedback;
    private final FeedbackRepository feedbackRepository;

    public FeedbackSlurper(SubscriberFileSenderConfig config) throws Exception {
        this(config, new SftpFeedback(config), new FeedbackRepository(config));
    }

    public FeedbackSlurper(SubscriberFileSenderConfig config, SftpFeedback sftpFeedback,
                           FeedbackRepository feedbackRepository) {
        this.config = config;
        this.sftpFeedback = sftpFeedback;
        this.feedbackRepository = feedbackRepository;
    }

    public void slurp() throws Exception {

        logger.info("**********");
        logger.info("Starting Process.");

        logger.info("**********");
        logger.info("Getting zip file(s) from SFTP.");
        FeedbackHolder feedbackHolder = sftpFeedback.getFeedbackHolder();

        logger.info("**********");
        logger.info("Processing success and failure results files.");
        processFiles( feedbackHolder );

        logger.info("**********");
        logger.info("Updating data_generator.subscriber_zip_file_uuids table.");

        logger.info("**********");
        logger.info("Cleaning up results staging directory.");
        cleanUp( feedbackHolder );

        logger.info("**********");
        logger.info("Process Completed.");

    }

    private void cleanUp(FeedbackHolder feedbackHolder) throws Exception {

        String resultsStagingDirString = config.getSubscriberFileLocationDetails().getResultsStagingDir();
        File resultsStagingDir = new File(resultsStagingDirString);
        FileUtils.cleanDirectory(resultsStagingDir);

    }

    private void processFiles(FeedbackHolder feedbackHolder) throws Exception {

        for (FileResult fileResult : feedbackHolder.getFileResults()) {

            // logger.info("Processing the file {}");

            for(Result result : fileResult.getResults()) {
                switch (result.getType()) {
                    case FAILURE:
                        feedbackRepository.processFailure(result);
                        break;
                    case SUCCESS:
                        feedbackRepository.processSuccess(result);
                        break;
                }
            }
        }
    }

    @Override
    public void close() throws Exception {
        sftpFeedback.close();
        feedbackRepository.close();
    }

}
