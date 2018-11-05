package org.endeavourhealth.scheduler.job;

import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.*;
// import java.net.URI;
import java.nio.file.*;

public class SendCsvFilesSFTP implements Job {

    private static final Logger LOG = LoggerFactory.getLogger(SendCsvFilesSFTP.class);



    public void execute(JobExecutionContext jobExecutionContext) {

    System.out.println("Sending Encrypted CSV files");

    //TODO Send encrypted CSV files via SFTP
    try {
        // URI sourceURI = URI.create("");
        // URI targetURI = URI.create("");
        Path sourcePath = Paths.get("C:/source");
        Path targetPath = Paths.get("C:/target");
        Files.copy(sourcePath, targetPath, StandardCopyOption.REPLACE_EXISTING);
        // Files.walkFileTree(sourcePath, new SimpleFileVisitor<>());
        System.out.println("CSV files sent");
    }
    catch (IOException e){
        System.out.println("I/O error occurred" + " " + e);
    }
    // System.out.println("CSV files sent");
}
}
