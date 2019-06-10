package org.endeavourhealth.cegdatabasefilesender.feedback;

import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.SftpException;
import net.lingala.zip4j.core.ZipFile;
import net.lingala.zip4j.exception.ZipException;
import org.endeavourhealth.scheduler.json.SubscriberFileSenderDefinition.SubscriberFileSenderConfig;
import org.endeavourhealth.scheduler.util.ConnectionDetails;
import org.endeavourhealth.scheduler.util.SftpConnection;
import org.endeavourhealth.scheduler.util.SftpConnectionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.ZoneId;
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


    List<Path> getPaths() throws SftpConnectionException, JSchException, IOException, SftpException, ZipException {

        sftp.open();

        ChannelSftp channelSftp = sftp.getChannel();

        List<Path> paths = new ArrayList<>();

        Vector<ChannelSftp.LsEntry>  fileList = channelSftp.ls("/endeavour/ftp/Remote_Server/result");

        List<ChannelSftp.LsEntry> filteredFiles = fileList
                .stream()
                .filter(t -> !t.getAttrs().isDir())
                .collect(Collectors.toList());

        for(ChannelSftp.LsEntry entry : filteredFiles) {
            logger.debug("Retrieving file {}", entry.getFilename());

            channelSftp.get("/endeavour/ftp/Remote_Server/result/" + entry.getFilename(), destinationPath + entry.getFilename());

            Path path = Paths.get("/tmp/" + entry.getFilename() );

            paths.add( path );

        }

        for (Path path : paths) {

            File file = path.toFile();

            logger.info("Deflating zip file {}", file.getName());

            ZipFile zipFile = new ZipFile(file);

            String destPath = "/tmp/" + file.getName().substring(0, file.getName().length() - 4);

            zipFile.extractAll(destPath);
        }

        return paths;
    }

    private ConnectionDetails getConnectionDetails() {
        String hostname = config.getSftpConnectionDetails().getHostname();

        int port = config.getSftpConnectionDetails().getPort();

        String username = config.getSftpConnectionDetails().getUsername();


        String clientPrivateKey = config.getSftpConnectionDetails().getClientPrivateKey();

        String clientPrivateKeyPassword = config.getSftpConnectionDetails().getClientPrivateKeyPassword();

        // String hostPublicKey = "";
        String hostPublicKey = config.getSftpConnectionDetails().getHostPublicKey();

//        clientPrivateKey = "-----BEGIN OPENSSH PRIVATE KEY-----\n" +
//                "b3BlbnNzaC1rZXktdjEAAAAABG5vbmUAAAAEbm9uZQAAAAAAAAABAAAAMwAAAAtzc2gtZW\n" +
//                "QyNTUxOQAAACB17ndfb6+3i48UN05YRB3YCrOT28riNLvaYdr3pHhjSwAAAJB9TWKofU1i\n" +
//                "qAAAAAtzc2gtZWQyNTUxOQAAACB17ndfb6+3i48UN05YRB3YCrOT28riNLvaYdr3pHhjSw\n" +
//                "AAAEB0Vzsrewf5swYj0Kjl7u5RyZe1N6f/yFkrNIyzzKAp1nXud19vr7eLjxQ3TlhEHdgK\n" +
//                "s5PbyuI0u9ph2vekeGNLAAAAB2hhbEBoYWwBAgMEBQY=\n" +
//                "-----END OPENSSH PRIVATE KEY-----";

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
