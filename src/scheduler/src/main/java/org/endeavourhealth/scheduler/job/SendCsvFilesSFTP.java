package org.endeavourhealth.scheduler.job;

import org.endeavourhealth.sftpreader.sources.ConnectionActivator;
import org.endeavourhealth.sftpreader.sources.ConnectionDetails;
import org.endeavourhealth.sftpreader.sources.Connection;
import org.endeavourhealth.sftpreader.utilities.RemoteFile;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.*;
import java.nio.file.Files;
import java.util.List;

public class SendCsvFilesSFTP implements Job {

    private static final Logger LOG = LoggerFactory.getLogger(SendCsvFilesSFTP.class);

    public void execute(JobExecutionContext jobExecutionContext) {

        System.out.println("Sending encrypted CSV files");

        //TODO Send encrypted CSV files via SFTP

        // This is the overall structure for connecting to an SFTP

        // Setting up the connection details
        String hostname = "10.0.101.239";
        // Hostname IP Address 35.176.117.37 (internet) 10.0.101.239 (vpn)
        // Hostname DNS ReverseLookup ec2-35-176-117-37.eu-west-2.compute.amazonaws.com
        // Hostname (created by Sophie) devsftp.endeavourhealth.net
        int port = 22;
        String username = "endeavour";

        String clientPrivateKey = null;
        try {clientPrivateKey = this.readClientPrivateKeyFile(
                "C:/sftpkey/sftp02endeavour.ppk");}
        catch (Exception e) { System.out.println(
                "Exception occurred while reading clientPrivateKey file. " + e);}
        // System.out.println(clientPrivateKey);

        String clientPrivateKeyPassword = "";
        String hostPublicKey = "";

        ConnectionDetails sftpConnectionDetails;
        sftpConnectionDetails = new ConnectionDetails();
        sftpConnectionDetails.setHostname(hostname);
        sftpConnectionDetails.setPort(port);
        sftpConnectionDetails.setUsername(username);
        sftpConnectionDetails.setClientPrivateKey(clientPrivateKey);
        sftpConnectionDetails.setClientPrivateKeyPassword(clientPrivateKeyPassword);
        sftpConnectionDetails.setHostPublicKey(hostPublicKey);

        // The sftpConnection object
        Connection sftpConnection = null;

        try {
            // Try opening the connection to the SFTP, reading the directory contents,
            // getting a CSV file from the SFTP directory and putting it onto the C drive,
            // and getting a file from the C drive and putting it onto the SFTP directory

            // Opening a connection to the SFTP
            sftpConnection = ConnectionActivator.createConnection(sftpConnectionDetails);
            System.out.println("Opening " + sftpConnection.getClass().getName() + " to " + hostname +
                    " on port " + port + " with user " + username);
            sftpConnection.open();
            System.out.println("Connected to the SFTP");

            // Reading the SFTP directory contents
            List<RemoteFile> remoteFileList =
                    sftpConnection.getFileList("/endeavour/ftp/"); // This is the remote path
            System.out.println(remoteFileList);

            // Getting a file from the SFTP directory ...
            InputStream inputStream = sftpConnection.getFile("/endeavour/ftp/test1onsftp.csv");
            System.out.println("Getting file from SFTP");

            // ... and putting it onto the C drive
            File downloadDestination = new File("C:/sftpdownload/test1fromsftp.csv");
            Files.copy(inputStream, downloadDestination.toPath());
            System.out.println("Downloading file to: " + downloadDestination);

            // Getting a file from the C drive and putting it onto the SFTP directory
            String localPath = "C:/sftpupload/test1oncdrive.csv";
            String destinationPath = "/endeavour/ftp/test1fromcdrive.csv"; // remote path
            // sftpConnection.put(localPath, destinationPath);
        }
        catch (Exception e) {
            // Catch if there is a problem while connecting to, or using, the SFTP
            // LOG.error("Exception occurred with using the SFTP." + e);
            System.out.println("Exception occurred with using the SFTP. " + e);
        }
        finally {
            // Close the connection to the SFTP
            if (sftpConnection != null)
                sftpConnection.close();
        }

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
        System.out.println("CSV files sent");
    }

    private String readClientPrivateKeyFile(String file) throws IOException {
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
    }
}