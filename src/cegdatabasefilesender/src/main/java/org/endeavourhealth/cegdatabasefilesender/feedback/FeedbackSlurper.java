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

        List<Result> resultsMarkedForDeletion = processFiles( feedbackHolder );

        deleteFiles( resultsMarkedForDeletion );

    }

    private List<Result> processFiles(FeedbackHolder feedbackHolder) throws Exception {

        List<Result> resultsMarkedForDeletion = new ArrayList<>();

        for (FailureResult failureResult : feedbackHolder.getFailureResults()) {
            feedbackRepository.process( failureResult, feedbackHolder );
            resultsMarkedForDeletion.add( failureResult );
        }

        for (SuccessResult successResult : feedbackHolder.getSuccessResults()) {
            feedbackRepository.process( successResult, feedbackHolder );
            resultsMarkedForDeletion.add( successResult );
        }

        return resultsMarkedForDeletion;
    }

    private void deleteFiles(List<Result> resultsMarkedForDeletion) {

    }

    @Override
    public void close() throws Exception {
        sftpFeedback.close();
        feedbackRepository.close();
    }

}
