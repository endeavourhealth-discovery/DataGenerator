package org.endeavourhealth.scheduler.job;

import org.apache.commons.io.FileUtils;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.*;
// import java.net.URI;
import java.net.URI;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.List;

public class SendCsvFilesSFTP implements Job {

    private static final Logger LOG = LoggerFactory.getLogger(SendCsvFilesSFTP.class);



    public void execute(JobExecutionContext jobExecutionContext) {

    System.out.println("Sending Encrypted CSV files");

    //TODO Send encrypted CSV files via SFTP
    try {
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

        System.out.println("CSV files sent");
    }
    catch (IOException e){
        System.out.println("I/O error occurred" + " " + e);
    }
    // System.out.println("CSV files sent");
}
}