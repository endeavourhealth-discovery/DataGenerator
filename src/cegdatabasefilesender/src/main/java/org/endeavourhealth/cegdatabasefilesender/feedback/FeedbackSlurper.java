package org.endeavourhealth.cegdatabasefilesender.feedback;

import org.endeavourhealth.cegdatabasefilesender.feedback.bean.FailureResult;
import org.endeavourhealth.cegdatabasefilesender.feedback.bean.FeedbackHolder;
import org.endeavourhealth.cegdatabasefilesender.feedback.bean.Result;
import org.endeavourhealth.cegdatabasefilesender.feedback.bean.SuccessResult;
import org.endeavourhealth.scheduler.json.SubscriberFileSenderDefinition.SubscriberFileSenderConfig;

import java.util.ArrayList;
import java.util.List;

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

        FeedbackHolder feedbackHolder = sftpFeedback.getFeedback();

        processFiles( feedbackHolder );

        cleanUp( feedbackHolder );

    }

    private void cleanUp(FeedbackHolder feedbackHolder) {
    }

    private void processFiles(FeedbackHolder feedbackHolder) throws Exception {

        for (FailureResult failureResult : feedbackHolder.getFailureResults()) {
            feedbackRepository.process( failureResult );
            resultsMarkedForDeletion.add( failureResult );
        }

        for (SuccessResult successResult : feedbackHolder.getSuccessResults()) {
            feedbackRepository.process( successResult );
            resultsMarkedForDeletion.add( successResult );
        }
    }

    @Override
    public void close() throws Exception {
        sftpFeedback.close();
        feedbackRepository.close();
    }

}
