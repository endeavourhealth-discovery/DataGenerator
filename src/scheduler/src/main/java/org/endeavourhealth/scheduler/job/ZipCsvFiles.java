package org.endeavourhealth.scheduler.job;

import net.lingala.zip4j.core.ZipFile;
import net.lingala.zip4j.model.ZipParameters;
import net.lingala.zip4j.util.Zip4jConstants;
import org.endeavourhealth.scheduler.cache.ExtractCache;
import org.endeavourhealth.scheduler.json.ExtractConfig;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

public class ZipCsvFiles implements Job {

    private static final Logger LOG = LoggerFactory.getLogger(ZipCsvFiles.class);

    private static final long ZIP_SPLIT_SIZE = 2621440; // 5242880 // 10485760

    public void execute(JobExecutionContext jobExecutionContext) {

        System.out.println("Zipping CSV files");
        LOG.info("Zipping CSV files");

        try {
            // Calling the file location config from the database
            ExtractConfig config = ExtractCache.getExtractConfig(1);
            // System.out.println(config.getName());

            try {
                // String sourceLocation = config.getFileLocationDetails().getSource();
                // String sourcePath = "C:/ziplocation/largefiletest1m32mb.csv";
                // File sourceFile = new File(sourcePath);
                // File destinationZipFolder = new File (sourceFile.getParent(),
                //        sourceFile.getName() + ".zip");
                // ZipFile zip = new ZipFile(destinationZipFolder);
                // zip.createZipFile(sourceFile, parameters, true, ZIP_SPLIT_SIZE);

                ZipFile zipFile = new ZipFile("C:/ziplocation/CreatedSplitZipFileFromFolder.zip");

                ZipParameters parameters = new ZipParameters();
                parameters.setCompressionMethod(Zip4jConstants.COMP_DEFLATE);
                parameters.setCompressionLevel(Zip4jConstants.DEFLATE_LEVEL_NORMAL);
                parameters.setIncludeRootFolder(false);

                zipFile.createZipFileFromFolder("C:/ziplocation", parameters, true, ZIP_SPLIT_SIZE);
            }
            catch (Exception e) {
                // Catch if there is a problem while creating the zip folder
                System.out.println("Exception occurred with creating the zip folder: " + e);
                LOG.error("Exception occurred with creating the zip folder: " + e);
            }
        } catch (Exception e) {
            System.out.println("Exception occurred with using the config database: " + e);
            LOG.error("Exception occurred with using the config database: " + e);
        }
    }
}