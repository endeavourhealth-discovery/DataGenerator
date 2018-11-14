package org.endeavourhealth.scheduler.job;

import org.endeavourhealth.scheduler.cache.ExtractCache;
import org.endeavourhealth.scheduler.json.ExtractConfig;
import org.endeavourhealth.sftpreader.sources.ConnectionActivator;
import org.endeavourhealth.sftpreader.sources.ConnectionDetails;
import org.endeavourhealth.sftpreader.sources.Connection;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.*;
import java.nio.file.Files;

public class SendCsvFilesSFTP implements Job {

    private static final Logger LOG = LoggerFactory.getLogger(SendCsvFilesSFTP.class);

    public void execute(JobExecutionContext jobExecutionContext) {

        System.out.println("Sending encrypted CSV files");

        //TODO Send encrypted CSV files via SFTP

        try {
            // Calling the SFTP config from the database
            ExtractConfig config = ExtractCache.getExtractConfig(1);
            // System.out.println(config.getName());

            // Setting up the connection details
            ConnectionDetails sftpConnectionDetails = this.setExtractConfigSftpConnectionDetails(config);

            // Creating the sftpConnection object
            Connection sftpConnection = null;

            try {
                // Try opening the connection to the SFTP,
                // (downloading a file from the SFTP and putting it onto the C drive,)
                // and getting a file from the C drive and uploading it onto the SFTP

                // Opening a connection to the SFTP
                sftpConnection = this.openSftpConnection(sftpConnectionDetails);

                // Downloading a file from the SFTP and putting it onto the C drive
                // this.downloadFileFromSftp(sftpConnection);

                // Getting a file from the C drive and uploading it onto the SFTP
                // String sourceLocalPath = "C:/sftpupload/test1oncdrive.csv";
                // String uploadDestinationPath = "/endeavour/ftp/test1fromcdrive.csv";
                String sourceLocalPath = config.getFileLocationDetails().getSource();
                String uploadDestinationPath = config.getFileLocationDetails().getDestination();

                this.uploadFileToSftp(sftpConnection, sourceLocalPath, uploadDestinationPath);

            } catch (Exception e) {
                // Catch if there is a problem while connecting to, or using, the SFTP
                // LOG.error("Exception occurred with using the SFTP." + e);
                System.out.println("Exception occurred with using the SFTP. " + e);
            } finally {
                // Close the connection to the SFTP
                if (sftpConnection != null)
                    sftpConnection.close();
            }
            System.out.println("CSV files sent");
        }
        catch (Exception e){
            System.out.println("Exception occurred with using the config database: " + e);
        }
    }

    private ConnectionDetails setExtractConfigSftpConnectionDetails(ExtractConfig config){
            // Setting up the connection details

            // Hostname IP Address 35.176.117.37 (internet) 10.0.101.239 (vpn)
            // Hostname DNS ReverseLookup ec2-35-176-117-37.eu-west-2.compute.amazonaws.com
            // Hostname (created by Sophie) devsftp.endeavourhealth.net

            // String hostname = "10.0.101.239";
            String hostname = config.getSftpConnectionDetails().getHostname();

            // int port = 22;
            int port = config.getSftpConnectionDetails().getPort();

            // String username = "endeavour";
            String username = config.getSftpConnectionDetails().getUsername();

            /* String clientPrivateKey = null;
            try {
                clientPrivateKey = this.readClientPrivateKeyFile(
                        "C:/sftpkey/sftp02endeavour.ppk");
            } catch (Exception e) {
                System.out.println(
                        "Exception occurred while reading clientPrivateKey file. " + e);
            } */
            // System.out.println(clientPrivateKey);
            String clientPrivateKey = config.getSftpConnectionDetails()
                    .getClientPrivateKey();

            // String clientPrivateKeyPassword = "";
            String clientPrivateKeyPassword = config.getSftpConnectionDetails()
                    .getClientPrivateKeyPassword();

            // String hostPublicKey = "";
            String hostPublicKey = config.getSftpConnectionDetails().getHostPublicKey();

            ConnectionDetails sftpConnectionDetails;
            sftpConnectionDetails = new ConnectionDetails();
            sftpConnectionDetails.setHostname(hostname);
            sftpConnectionDetails.setPort(port);
            sftpConnectionDetails.setUsername(username);
            sftpConnectionDetails.setClientPrivateKey(clientPrivateKey);
            sftpConnectionDetails.setClientPrivateKeyPassword(clientPrivateKeyPassword);
            sftpConnectionDetails.setHostPublicKey(hostPublicKey);
            return sftpConnectionDetails;
    }

    private Connection openSftpConnection(ConnectionDetails sftpConnectionDetails) throws Exception {
        // Opening a connection to the SFTP
        Connection sftpConnection = null;
        sftpConnection = ConnectionActivator.createConnection(sftpConnectionDetails);
        System.out.println("Opening " + sftpConnection.getClass().getName()
                + " to " + sftpConnectionDetails.getHostname()
                + " on port " + sftpConnectionDetails.getPort()
                + " with username " + sftpConnectionDetails.getUsername());
        sftpConnection.open();
        System.out.println("Connected to the SFTP");
        return sftpConnection;
    }

    private void uploadFileToSftp(Connection sftpConnection, String source, String destination) throws Exception {
        String sourceLocalPath = source;
        String uploadDestinationPath = destination;
        sftpConnection.put(sourceLocalPath, uploadDestinationPath);
        System.out.println("Uploaded file to SFTP: " + uploadDestinationPath);
    }

    private void downloadFileFromSftp(Connection sftpConnection) throws Exception {
        // Getting a file from the SFTP directory ...
        InputStream downloadInputStream = sftpConnection.getFile("/endeavour/ftp/test1onsftp.csv");
        System.out.println("Downloaded file from SFTP");

        // ... and putting it onto the C drive
        String downloadDestinationString = "C:/sftpdownload/test1fromsftp.csv";
        File downloadDestination = new File(downloadDestinationString);
        Files.copy(downloadInputStream, downloadDestination.toPath());
        System.out.println("Downloaded file written to: " + downloadDestination);
    }

    /* private String readClientPrivateKeyFile(String file) throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader (file));
        String line = null;
        StringBuilder stringBuilder = new StringBuilder();
        String ls = System.getProperty("line.separator");
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
    } */
}