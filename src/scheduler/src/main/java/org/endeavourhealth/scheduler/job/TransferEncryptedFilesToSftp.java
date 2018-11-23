package org.endeavourhealth.scheduler.job;

import org.endeavourhealth.scheduler.cache.ExtractCache;
import org.endeavourhealth.scheduler.json.ExtractConfig;
// import org.endeavourhealth.sftpreader.sources.Connection;
// import org.endeavourhealth.sftpreader.sources.ConnectionActivator;
// import org.endeavourhealth.sftpreader.sources.ConnectionDetails;
import org.endeavourhealth.scheduler.models.database.FileTransactionsEntity;
import org.endeavourhealth.scheduler.util.Connection;
import org.endeavourhealth.scheduler.util.ConnectionActivator;
import org.endeavourhealth.scheduler.util.ConnectionDetails;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// import java.io.*;
import java.io.File;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.List;

public class TransferEncryptedFilesToSftp implements Job {

    private static final Logger LOG = LoggerFactory.getLogger(TransferEncryptedFilesToSftp.class);

    public void execute(JobExecutionContext jobExecutionContext) {

        System.out.println("Transferring encrypted files");
        LOG.info("Transferring encrypted files");
        int[] extractIdArray = {1};
        for (int extractId : extractIdArray) {
            try {
                // Getting the extract config from the extract table of the database
                // int extractId = 1;
                ExtractConfig config = ExtractCache.getExtractConfig(extractId);
                System.out.println(config.getName());

                // Getting the file transaction details from the file_transactions table of the database
                List<FileTransactionsEntity> toProcess = FileTransactionsEntity.getFilesForSftp(extractId);
                if (toProcess == null || toProcess.size() == 0) {
                    System.out.println("No files for transfer to SFTP");
                    LOG.info("No files for transfer to SFTP");
                    return;
                }

                // Setting up the connection details
                ConnectionDetails sftpConnectionDetails = this.setExtractConfigSftpConnectionDetails(config);

                // Creating the sftpConnection object
                Connection sftpConnection = null;

                try {
                    // Opening a connection to the SFTP
                    sftpConnection = this.openSftpConnection(sftpConnectionDetails);

                    for (FileTransactionsEntity entry : toProcess) {

                        /*  // Getting a set of files from the extract definition, file location details,
                         *  // specified source folder, and uploading them to the similarly specified
                         *  // SFTP destination folder
                         *
                         *  String sourceLocation = config.getFileLocationDetails().getSource();
                         *  File sourceFolder = new File(sourceLocation);
                         *  String[] folderFilenamesStringArray = sourceFolder.list();
                         *  for (String filename : folderFilenamesStringArray) {
                         *      String sourcePath = sourceLocation + filename;
                         *      String destinationPath = config.getFileLocationDetails().getDestination()
                         *              + filename;
                         *       this.uploadFileToSftp(sftpConnection, sourcePath, destinationPath);
                         *  }
                         */

                        // Getting one file at a time and uploading it to the SFTP
                        String sourceLocation = config.getFileLocationDetails().getSource();
                        if (!(sourceLocation.endsWith(File.separator))) {
                            sourceLocation += File.separator;
                        }
                        String sourcePath = sourceLocation + entry.getFilename();

                        String destinationLocation = config.getFileLocationDetails().getDestination();
                        if (!(destinationLocation.endsWith(File.separator))) {
                            destinationLocation += File.separator;
                        }
                        String destinationPath = destinationLocation + entry.getFilename();

                        this.uploadFileToSftp(sftpConnection, sourcePath, destinationPath);

                        entry.setSftpDate(new Timestamp(System.currentTimeMillis()));
                        FileTransactionsEntity.update(entry);
                        System.out.println("File: " + entry.getFilename() + " record updated");
                        LOG.info("File: " + entry.getFilename() + " record updated");
                    }
                } catch (Exception e) {
                    // Catch if there is a problem while connecting to, or using, the SFTP
                    System.out.println("Exception occurred with using the SFTP: " + e);
                    LOG.error("Exception occurred with using the SFTP: " + e);
                } finally {
                    // Close the connection to the SFTP
                    if (sftpConnection != null)
                        sftpConnection.close();
                }
            } catch (Exception e) {
                System.out.println("Exception occurred with using the database: " + e);
                LOG.error("Exception occurred with using the database: " + e);
            }
        }
    }

    public ConnectionDetails setExtractConfigSftpConnectionDetails(ExtractConfig config) {
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

    public Connection openSftpConnection(ConnectionDetails sftpConnectionDetails) throws Exception {
        // Opening a connection to the SFTP
        Connection sftpConnection = null;
        sftpConnection = ConnectionActivator.createConnection(sftpConnectionDetails);
        System.out.println("Opening SFTP Connection" // + sftpConnection.getClass().getName()
                + " to " + sftpConnectionDetails.getHostname()
                + " on Port " + sftpConnectionDetails.getPort()
                + " with Username " + sftpConnectionDetails.getUsername());
        LOG.info("Opening SFTP Connection" // + sftpConnection.getClass().getName()
                + " to " + sftpConnectionDetails.getHostname()
                + " on port " + sftpConnectionDetails.getPort()
                + " with username " + sftpConnectionDetails.getUsername());
        sftpConnection.open();
        System.out.println("Connected to this SFTP");
        LOG.info("Connected to this SFTP");
        return sftpConnection;
    }

    public void uploadFileToSftp(Connection sftpConnection, String source, String destination) throws Exception {
        String sourcePath = source;
        String destinationPath = destination;

        Calendar startCalendar = Calendar.getInstance();
        System.out.println("Tried starting upload of file " + sourcePath + " on " + startCalendar.getTime() + " to SFTP: " + destinationPath);
        LOG.info("Tried starting upload of file " + sourcePath + " on " + startCalendar.getTime() + " to SFTP: " + destinationPath);

        sftpConnection.put(sourcePath, destinationPath);

        Calendar endCalendar = Calendar.getInstance();
        System.out.println("Finished uploading file " + sourcePath + " on " + endCalendar.getTime() + " to SFTP: " + destinationPath);
        LOG.info("Finished uploading file " + sourcePath + " on " + endCalendar.getTime() + " to SFTP: " + destinationPath);
    }

    /* private void downloadFileFromSftp(Connection sftpConnection) throws Exception {
        // Getting a file from the SFTP directory ...
        InputStream downloadInputStream = sftpConnection.getFile("/endeavour/ftp/test1onsftp.csv");
        System.out.println("Downloaded file from SFTP");

        // ... and putting it onto the C drive
        String downloadDestinationString = "C:/sftpdownload/test1fromsftp.csv";
        File downloadDestination = new File(downloadDestinationString);
        Files.copy(downloadInputStream, downloadDestination.toPath());
        System.out.println("Downloaded file written to: " + downloadDestination);
    } */

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