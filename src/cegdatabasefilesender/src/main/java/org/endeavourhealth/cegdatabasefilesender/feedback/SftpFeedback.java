package org.endeavourhealth.cegdatabasefilesender.feedback;

import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.SftpException;
import net.lingala.zip4j.core.ZipFile;
import org.apache.commons.io.FileUtils;
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
    private String destinationPath = null;

    private static final Logger logger = LoggerFactory.getLogger(SftpFeedback.class);

    public SftpFeedback(SubscriberFileSenderConfig config) throws Exception {

        this.config = config;
        ConnectionDetails con = getConnectionDetails();
        this.sftp = new SftpConnection(con);

        String resultsStagingDirString = this.config.getSubscriberFileLocationDetails().getResultsStagingDir();
        if (!(resultsStagingDirString.endsWith(File.separator))) {
            resultsStagingDirString += File.separator;
        }
        this.destinationPath = resultsStagingDirString;

        File resultsstagingDir = new File(resultsStagingDirString);
        if (!(resultsstagingDir.exists())) {
            resultsstagingDir.mkdirs();
        }

    }

    FeedbackHolder getFeedbackHolder() throws Exception {

        List<Path> paths = getPaths();
        FeedbackHolder holder = new FeedbackHolder();

            if (!(paths.isEmpty())) {
                holder = getFeedbackHolder(paths);
                return holder;
            }

        return holder;
    }

    private FeedbackHolder getFeedbackHolder(List<Path> paths) throws Exception {

        FeedbackHolder feedbackHolder = new FeedbackHolder();

        for (Path path : paths) {

            File file = path.toFile();

            String destPath = unzip(file);

            FileResult fileResult = new FileResult(destPath);

            File successFile = new File(destPath + "/success.txt");
            if (successFile.exists()) {

                try {
                    String success = new String(Files.readAllBytes(Paths.get(destPath + "/success.txt")));
                    fileResult.addSuccess(success);
                } catch (Exception e) {
                    logger.error("Cannot read success.txt", e);
                    fileResult.addError("Cannot read success.txt for " + destPath);
                }
            }

            File failureFile = new File(destPath + "/failure.txt");
            if (failureFile.exists()) {

                try {
                    String failure = new String(Files.readAllBytes(Paths.get(destPath + "/failure.txt")));
                    fileResult.addFailure(failure);
                } catch (Exception e) {
                    logger.error("Cannot read failure.txt", e);
                    fileResult.addError("Cannot read failure.txt for " + destPath);
                }
            }

            feedbackHolder.addFileResult(fileResult);
        }

        return feedbackHolder;
    }

    private String unzip(File file) throws Exception {
        logger.info("Unzipping the file {}", file.getName());

        ZipFile zipFile = new ZipFile(file);

        String filepath = file.getName().substring(0, file.getName().length() - 4);
        String destPath = destinationPath + filepath;

        zipFile.extractAll(destPath);

        String archiveDirString = config.getSubscriberFileLocationDetails().getArchiveDir();
        if (!(archiveDirString.endsWith(File.separator))) {
            archiveDirString += File.separator;
        }

        logger.info("Archiving the file {}", file.getName());
        File archiveDir = new File(archiveDirString);
        if (!(archiveDir.exists())) {
            archiveDir.mkdirs();
        }

        FileUtils.copyFileToDirectory(file, archiveDir);
        System.gc();
        Thread.sleep(1000);
        Path path = file.toPath();
        Files.delete(path);
        // FileUtils.forceDelete(file);

        return destPath;
    }

    private List<Path> getPaths() throws JSchException, IOException, SftpConnectionException, SftpException {

        List<Path> paths = new ArrayList<>();

        sftp.open();

        ChannelSftp channelSftp = sftp.getChannel();
        String resultsSourceDir = config.getSubscriberFileLocationDetails().getResultsSourceDir();

        Vector<ChannelSftp.LsEntry> fileList = channelSftp.ls(resultsSourceDir);

        List<ChannelSftp.LsEntry> filteredFiles = fileList
                .stream()
                .filter(t -> !t.getAttrs().isDir())
                .collect(Collectors.toList());

        if (filteredFiles.size() == 0) {
            logger.info("SFTP location is empty.");

        } else {
            for (ChannelSftp.LsEntry entry : filteredFiles) {

                logger.info("Retrieving from SFTP the file {}", entry.getFilename());

                String sourcePath = resultsSourceDir + entry.getFilename();
                channelSftp.get(sourcePath, destinationPath + entry.getFilename());
                Path path = Paths.get(destinationPath + entry.getFilename());
                paths.add(path);

                logger.info("Removing from SFTP the file {}", entry.getFilename());

                channelSftp.rm(sourcePath);

            }
            return paths;
        }
        return paths;

    }

    private ConnectionDetails getConnectionDetails() throws Exception {

        String hostname = config.getSftpConnectionDetails().getHostname();
        int port = config.getSftpConnectionDetails().getPort();
        String username = config.getSftpConnectionDetails().getUsername();

        // String clientPrivateKey = config.getSftpConnectionDetails().getClientPrivateKey();
        File privateKeyFile = new File(config.getSubscriberFileLocationDetails().getPrivateKeyFile());
        String clientPrivateKey = FileUtils.readFileToString(privateKeyFile, (String) null);

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
