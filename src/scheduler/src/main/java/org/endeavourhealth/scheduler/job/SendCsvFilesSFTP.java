package org.endeavourhealth.scheduler.job;

import com.jcraft.jsch.JSchException;
import org.apache.commons.io.FileUtils;
import org.endeavourhealth.sftpreader.model.db.DbConfigurationSftp;
import org.endeavourhealth.sftpreader.utilities.sftp.SftpConnection;
import org.endeavourhealth.sftpreader.utilities.sftp.SftpConnectionDetails;
import org.endeavourhealth.sftpreader.utilities.sftp.SftpConnectionException;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.*;
import java.net.URI;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.List;

public class SendCsvFilesSFTP implements Job {

    private static final Logger LOG = LoggerFactory.getLogger(SendCsvFilesSFTP.class);

    private final String readFile(String file) throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader (file));
        String         line = null;
        StringBuilder  stringBuilder = new StringBuilder();
        String         ls = System.getProperty("line.separator");
        try {
            while((line = reader.readLine()) != null) {
                stringBuilder.append(line);
                stringBuilder.append(ls);
            }
            return stringBuilder.toString();
        }
        finally {
            reader.close();
        }
    }

    public void execute(JobExecutionContext jobExecutionContext) {

        System.out.println("Sending Encrypted CSV files");

        //TODO Send encrypted CSV files via SFTP

        // This is the overall structure for connecting to an SFTP

        // Hostname IP Address 35.176.117.37
        // Hostname DNS ReverseLookup ec2-35-176-117-37.eu-west-2.compute.amazonaws.com
        // Hostname (created by Sophie) https://devsftp.endeavourhealth.net/
        String hostname = "devsftp.endeavourhealth.net";
        int port = 22;
        // username = endeavour
        String username = "endeavour";

        String clientPrivateKey = null;
        try {clientPrivateKey = this.readFile("C:/sftpkey/sftp02endeavour.ppk");}
        catch (Exception e) { System.out.println("Exception occurred while reading clientPrivateKey file. " + e);}
        // System.out.println(clientPrivateKey);

        // String clientPrivateKeyPassword = null;
        // String hostPublicKey = null;

        SftpConnectionDetails sftpconnectiondetails;
        SftpConnection sftpconnection;

        sftpconnectiondetails = new SftpConnectionDetails();
        sftpconnectiondetails.setHostname(hostname);
        sftpconnectiondetails.setPort(port);
        sftpconnectiondetails.setUsername(username);
        sftpconnectiondetails.setClientPrivateKey(clientPrivateKey);
        // sftpconnectiondetails.setClientPrivateKeyPassword(clientPrivateKeyPassword);
        // sftpconnectiondetails.setHostPublicKey(hostPublicKey);

        sftpconnection = new SftpConnection(sftpconnectiondetails);

        try {
            // Try opening the connection to the SFTP and writing to it
            sftpconnection.open();
            // String localPath = "C:/sftpupload";
            // String destinationPath = sftpconnectiondetails.getKnownHostsString();
            // sftpconnection.put(localPath, destinationPath);
            // sftpconnection.getFile(destinationPath);
        }
        catch (Exception e) {
            // Catch if there is a problem connecting to it
            // LOG.error("Exception occurred while connecting to the SFTP." + e);
            System.out.println("Exception occurred while connecting to the SFTP. " + e);
        }
        finally {
            // Close the connection to the SFTP
            if (sftpconnection != null)
                sftpconnection.close();
        }

        /* Helper methods from SftpReaderTask

        private static SftpConnection openSftpConnection (DbConfigurationSftp configurationSftp) throws SftpConnectionException, JSchException, IOException {
           SftpConnection sftpConnection = new SftpConnection(getSftpConnectionDetails(configurationSftp));

            String hostname = sftpConnection.getConnectionDetails().getHostname();
            String port = Integer.toString(sftpConnection.getConnectionDetails().getPort());
            String username = sftpConnection.getConnectionDetails().getUsername();

            LOG.info(" Opening SFTP connection to " + hostname + " on port " + port + " with user " + username);

            sftpConnection.open();

            return sftpConnection;
        }

        private static SftpConnectionDetails getSftpConnectionDetails(DbConfigurationSftp configurationSftp) {
            return new SftpConnectionDetails()
                    .setHostname(configurationSftp.getHostname())
                    .setPort(configurationSftp.getPort())
                    .setUsername(configurationSftp.getUsername())
                    .setClientPrivateKey(configurationSftp.getClientPrivateKey())
                    .setClientPrivateKeyPassword(configurationSftp.getClientPrivateKeyPassword())
                    .setHostPublicKey(configurationSftp.getHostPublicKey());
        }

        private static void closeConnection(SftpConnection sftpConnection) {
            if (sftpConnection != null)
                sftpConnection.close();
        }

        */

        /* try {
            // Don't know where the files will be after encryption, or where
            // they are going to, so will need to use URIs rather hard-coded paths
            // URI sourceURI = URI.create("C:/source");
            // URI targetURI = URI.create("C:/target");

            // This takes the source directory folder and recreates it and
            // its contents in the target location, overwriting everything
            String source = "C:/source";
            File sourceDir = new File(source);
            String target = "C:/target";
            File targetDir = new File(target);
            FileUtils.copyDirectory(sourceDir, targetDir);

            // This just takes the source directory folder and only creates the target
            // folder if none is already there, otherwise there's an exception
            // Path sourcePath = Paths.get("C:/source");
            // Path targetPath = Paths.get("C:/target");
            // Files.copy(sourcePath, targetPath, StandardCopyOption.REPLACE_EXISTING);

            // Still thinking about this - want to get a collection of files from a specific
            // directory, which can then be iteratively uploaded to an SFTP, or can you just
            // upload a directory all in one hit?
            // List<File> files = new ArrayList<>();
            // files.add(Files.walkFileTree(sourcePath, new SimpleFileVisitor<>())); - doesn't work
            //
            System.out.println("CSV files sent");
        } catch (IOException e) {
            System.out.println("I/O error occurred" + " " + e);
        }
        */
        // System.out.println("CSV files sent");
    }
}