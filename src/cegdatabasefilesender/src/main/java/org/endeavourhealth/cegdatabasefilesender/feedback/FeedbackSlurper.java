package org.endeavourhealth.cegdatabasefilesender.feedback;

import org.endeavourhealth.cegdatabasefilesender.feedback.bean.FeedbackHolder;
import org.endeavourhealth.cegdatabasefilesender.feedback.bean.FileResult;
import org.endeavourhealth.cegdatabasefilesender.feedback.bean.Result;
import org.endeavourhealth.scheduler.json.SubscriberFileSenderDefinition.SubscriberFileSenderConfig;

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

    private void cleanUp(FeedbackHolder feedbackHolder) {
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
