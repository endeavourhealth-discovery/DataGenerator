package org.endeavourhealth.cegdatabasefilesender.feedback;

import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.SftpException;
import net.lingala.zip4j.exception.ZipException;
import org.endeavourhealth.scheduler.json.SubscriberFileSenderDefinition.SubscriberFileSenderConfig;
import org.endeavourhealth.scheduler.util.SftpConnectionException;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public class FeedbackSlurper implements AutoCloseable {

    private final SubscriberFileSenderConfig config;

    private final SftpFeedback sftpFeedback;

    private final FeedbackRepository feedbackRepository;

    public FeedbackSlurper(SubscriberFileSenderConfig config) throws Exception {
        this(config, new SftpFeedback(config), new FeedbackRepository(config));
    }

    public FeedbackSlurper(SubscriberFileSenderConfig config, SftpFeedback sftpFeedback, FeedbackRepository feedbackRepository) {
        this.config = config;
        this.sftpFeedback = sftpFeedback;
        this.feedbackRepository = feedbackRepository;
    }

    public void slurp() throws Exception {

        List<Path> paths = sftpFeedback.getPaths();

        List<Path> filesMarkedForDeletion = processFiles(paths);

        deleteFiles(filesMarkedForDeletion);
    }


    private List<Path> processFiles(List<Path> files) throws Exception {

        List<Path> filesMarkedForDeletion = new ArrayList<>();

        for (Path file : files) {
            feedbackRepository.process(file);
            filesMarkedForDeletion.add(file);
        }

        return filesMarkedForDeletion;
    }


    private void deleteFiles(List<Path> filesMarkedForDeletion) {

    }

    @Override
    public void close() throws Exception {
        sftpFeedback.close();
        feedbackRepository.close();
    }
}
