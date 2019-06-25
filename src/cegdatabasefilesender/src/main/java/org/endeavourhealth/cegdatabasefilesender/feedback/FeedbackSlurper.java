package org.endeavourhealth.cegdatabasefilesender.feedback;

import org.apache.commons.io.FileUtils;
import org.endeavourhealth.cegdatabasefilesender.feedback.bean.FeedbackHolder;
import org.endeavourhealth.cegdatabasefilesender.feedback.bean.FileResult;
import org.endeavourhealth.cegdatabasefilesender.feedback.bean.Result;
import org.endeavourhealth.common.utility.SlackHelper;
import org.endeavourhealth.scheduler.json.SubscriberFileSenderDefinition.SubscriberFileSenderConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
// import java.nio.file.Files;
// import java.nio.file.Path;

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

        SlackHelper.setupConfig("", "",
                SlackHelper.Channel.RemoteFilerAlerts.getChannelName(),
                "https://hooks.slack.com/services/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");

    }

    public void slurp(int subscriberId) throws Exception {

        logger.info("**********");
        logger.info("Start of feedback process for subscriber_id {}.", subscriberId);
        SlackHelper.sendSlackMessage(SlackHelper.Channel.RemoteFilerAlerts, "*** Start of feedback process for subscriber_id " + subscriberId);

        logger.info("**********");
        logger.info("Getting zip file(s) from SFTP.");
        SlackHelper.sendSlackMessage(SlackHelper.Channel.RemoteFilerAlerts, "Getting zip file(s) from SFTP.");

        FeedbackHolder feedbackHolder = sftpFeedback.getFeedbackHolder();

        if (feedbackHolder.getFileResults().isEmpty()) {
            logger.info("**********");
            logger.info("No feedback results to process.");
            SlackHelper.sendSlackMessage(SlackHelper.Channel.RemoteFilerAlerts,"No feedback results to process.");

        } else {

            logger.info("**********");
            logger.info("Processing success and failure of filing results files.");
            SlackHelper.sendSlackMessage(SlackHelper.Channel.RemoteFilerAlerts,"Processing success and failure of filing results files.");
            processFiles(feedbackHolder);

            logger.info("**********");
            logger.info("Updating data_generator.subscriber_zip_file_uuids table.");
            SlackHelper.sendSlackMessage(SlackHelper.Channel.RemoteFilerAlerts,"Updating data_generator.subscriber_zip_file_uuids table.");

            logger.info("**********");
            logger.info("Cleaning up results staging directory.");
            SlackHelper.sendSlackMessage(SlackHelper.Channel.RemoteFilerAlerts,"Cleaning up results staging directory.");
            cleanUp(feedbackHolder);

        }

        logger.info("**********");
        logger.info("End of feedback process for subscriber_id {}.", subscriberId);
        SlackHelper.sendSlackMessage(SlackHelper.Channel.RemoteFilerAlerts,"*** End of feedback process for subscriber_id " + subscriberId);

    }

    private void cleanUp(FeedbackHolder feedbackHolder) throws Exception {

        String resultsStagingDirString = config.getSubscriberFileLocationDetails().getResultsStagingDir();
        File resultsStagingDir = new File(resultsStagingDirString);
        FileUtils.cleanDirectory(resultsStagingDir);

        /* File[] files = resultsStagingDir.listFiles();

        for (File file : files) {

            System.gc();
            Thread.sleep(1000);
            Path filepath = file.toPath();
            Files.delete(filepath);
            // FileUtils.forceDelete(file);
        } */

    }

    private void processFiles(FeedbackHolder feedbackHolder) throws Exception {

        int successResults = 0;
        int failureResults = 0;

        for (FileResult fileResult : feedbackHolder.getFileResults()) {

            // logger.info("Processing the file {}");

            for(Result result : fileResult.getResults()) {
                switch (result.getType()) {
                    case FAILURE:
                        feedbackRepository.processFailure(result);
                        failureResults++;
                        break;
                    case SUCCESS:
                        feedbackRepository.processSuccess(result);
                        successResults++;
                        break;
                }
            }
        }

        logger.info("Successfully filed: " + successResults);
        logger.info("Unsuccessfully filed: " + failureResults);
        SlackHelper.sendSlackMessage(SlackHelper.Channel.RemoteFilerAlerts,"Successfully filed: " + successResults);
        SlackHelper.sendSlackMessage(SlackHelper.Channel.RemoteFilerAlerts,"Unsuccessfully filed: " + failureResults);

    }

    @Override
    public void close() {
        sftpFeedback.close();
        feedbackRepository.close();
    }

}
