package org.endeavourhealth.cegdatabasefilesender.feedback;

import org.apache.commons.io.FileUtils;
import org.endeavourhealth.cegdatabasefilesender.feedback.bean.FeedbackHolder;
import org.endeavourhealth.cegdatabasefilesender.feedback.bean.FileResult;
import org.endeavourhealth.cegdatabasefilesender.feedback.bean.Result;
import org.endeavourhealth.scheduler.json.SubscriberFileSenderDefinition.SubscriberFileSenderConfig;

import java.io.File;

public class FeedbackSlurper implements AutoCloseable {

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

        FeedbackHolder feedbackHolder = sftpFeedback.getFeedbackHolder();

        processFiles( feedbackHolder );

        cleanUp( feedbackHolder );

    }

    private void cleanUp(FeedbackHolder feedbackHolder) throws Exception {

        String resultsStagingDirString = config.getSubscriberFileLocationDetails().getResultsStagingDir();
        File resultsStagingDir = new File(resultsStagingDirString);
        FileUtils.cleanDirectory(resultsStagingDir);
        // TODO need to add clean up of SFTP as well

    }

    private void processFiles(FeedbackHolder feedbackHolder) throws Exception {

        for (FileResult fileResult : feedbackHolder.getFileResults()) {

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
