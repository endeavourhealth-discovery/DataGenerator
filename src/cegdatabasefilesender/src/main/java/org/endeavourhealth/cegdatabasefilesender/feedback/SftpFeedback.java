package org.endeavourhealth.cegdatabasefilesender.feedback;

import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.SftpException;
import net.lingala.zip4j.core.ZipFile;
import net.lingala.zip4j.exception.ZipException;
import org.endeavourhealth.cegdatabasefilesender.feedback.bean.*;
import org.endeavourhealth.scheduler.json.SubscriberFileSenderDefinition.SubscriberFileSenderConfig;
import org.endeavourhealth.scheduler.util.ConnectionDetails;
import org.endeavourhealth.scheduler.util.SftpConnection;
import org.endeavourhealth.scheduler.util.SftpConnectionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;


public class SftpFeedback {

    private final SubscriberFileSenderConfig config;
    private SftpConnection sftp;
    private String destinationPath = "/tmp/";

    private static final Logger logger = LoggerFactory.getLogger(SftpFeedback.class);

    public SftpFeedback(SubscriberFileSenderConfig config) {
        this.config = config;

        ConnectionDetails con = getConnectionDetails();

        sftp = new SftpConnection(con);
    }


    FeedbackHolder getFeedback() throws SftpConnectionException, JSchException, IOException, SftpException, ZipException {

        List<Path> paths = getPaths();

        FeedbackHolder holder = getFeedbackHolder(paths);

        return holder;
    }

    private FeedbackHolder getFeedbackHolder(List<Path> paths) throws ZipException {
        FeedbackHolder holder = null;

        List<String> successList = new ArrayList<>();
        List<String> failureList = new ArrayList<>();
        List<Result> errors = new ArrayList<>();

        for (Path path : paths) {

            File file = path.toFile();

            String destPath = unzip(file);

            try {
                String success = new String(Files.readAllBytes(Paths.get(destPath + "/success.txt")));
                successList.addAll( Arrays.asList( success.split("\r\n") ) );
            } catch(Exception e) {
                logger.error("Cannot read success.txt", e);
                errors.add( new ErrorResult("Cannot read success.txt for " + destPath)) ;
            }

            try {
                String failure = new String(Files.readAllBytes(Paths.get(destPath + "/failure.txt")));
                failureList.addAll( Arrays.asList( failure.split("\r\n") ) );
            } catch(Exception e) {
                logger.info("Cannot read failure.txt");
                errors.add( new ErrorResult("Cannot read failure.txt for " + destPath) );
            }

            List<FailureResult> failureResults = failureList.stream().map(str -> new FailureResult( str )).collect(Collectors.toList());
            List<SuccessResult> successResults = successList.stream().map(str -> new SuccessResult( str )).collect(Collectors.toList());

            holder = new FeedbackHolder( failureResults, successResults, errors);

        }
        return holder;
    }

    private String unzip(File file) throws ZipException {
        logger.info("Deflating zip file {}", file.getName());

        ZipFile zipFile = new ZipFile(file);

        String filepath = file.getName().substring(0, file.getName().length() - 4);
        String destPath = "/tmp/" + filepath;

        zipFile.extractAll(destPath);

        return destPath;
    }

    private List<Path> getPaths() throws JSchException, IOException, SftpConnectionException, SftpException {
        List<Path> paths = new ArrayList<>();

        sftp.open();

        ChannelSftp channelSftp = sftp.getChannel();
        String resultsDir = config.getSubscriberFileLocationDetails().getResultsDir();

        Vector<ChannelSftp.LsEntry> fileList = channelSftp.ls(resultsDir);

        List<ChannelSftp.LsEntry> filteredFiles = fileList
                .stream()
                .filter(t -> !t.getAttrs().isDir())
                .collect(Collectors.toList());

        for(ChannelSftp.LsEntry entry : filteredFiles) {
            logger.debug("Retrieving file {}", entry.getFilename());

            channelSftp.get(resultsDir + entry.getFilename(), destinationPath + entry.getFilename());

            Path path = Paths.get("/tmp/" + entry.getFilename() );

            paths.add( path );

        }
        return paths;
    }

    private ConnectionDetails getConnectionDetails() {

        String hostname = config.getSftpConnectionDetails().getHostname();
        int port = config.getSftpConnectionDetails().getPort();
        String username = config.getSftpConnectionDetails().getUsername();
        String clientPrivateKey = config.getSftpConnectionDetails().getClientPrivateKey();
        String clientPrivateKeyPassword = config.getSftpConnectionDetails().getClientPrivateKeyPassword();
        String hostPublicKey = config.getSftpConnectionDetails().getHostPublicKey();
        ConnectionDetails sftpConnectionDetails = new ConnectionDetails();

        sftpConnectionDetails.setHostname(hostname);
        sftpConnectionDetails.setPort(port);
        sftpConnectionDetails.setUsername(username);
        sftpConnectionDetails.setClientPrivateKey(clientPrivateKey);
        sftpConnectionDetails.setClientPrivateKeyPassword(clientPrivateKeyPassword);
        sftpConnectionDetails.setHostPublicKey(hostPublicKey);

        return sftpConnectionDetails;
    }

    public void close() {
        sftp.close();
    }
}
