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
import java.io.FileFilter;
import java.util.Calendar;
// import java.io.File;

public class ZipCsvFiles implements Job {

    private static final Logger LOG = LoggerFactory.getLogger(ZipCsvFiles.class);

    private static final long ZIP_SPLIT_SIZE = 2621440; // 5242880 // 10485760

    public void execute(JobExecutionContext jobExecutionContext) {

        System.out.println("Zipping CSV files contained in folders");
        LOG.info("Zipping CSV files contained in folders");

        int[] extractIdArray = {1, 2};
        for (int extractId : extractIdArray) {
            try {
                // Calling the file location config from the database
                ExtractConfig config = ExtractCache.getExtractConfig(extractId);
                System.out.println(config.getName());
                try {
                    // This turns a single file into a multi-part zip file
                    // String sourcePath = "C:/ziplocation/largefiletest1m32mb.csv";
                    // File sourceFile = new File(sourcePath);
                    // File destinationZipFolder = new File (sourceFile.getParent(),
                    // sourceFile.getName() + ".zip");
                    // ZipFile zip = new ZipFile(destinationZipFolder);
                    // zip.createZipFile(sourceFile, parameters, true, ZIP_SPLIT_SIZE);

                    // This takes the contents of a specified folder location (i.e. all its files)
                    // and creates a multi-part zip file in the same place: original files not deleted
                    String sourceLocation = config.getFileLocationDetails().getSource();

                    ZipFile zipFile = new ZipFile(sourceLocation + "CreatedSplitZipFileFromSourceFolderFiles.zip");

                    ZipParameters parameters = new ZipParameters();
                    parameters.setCompressionMethod(Zip4jConstants.COMP_DEFLATE);
                    parameters.setCompressionLevel(Zip4jConstants.DEFLATE_LEVEL_NORMAL);
                    parameters.setIncludeRootFolder(false);
                    Calendar startCalendar = Calendar.getInstance();
                    System.out.println("Tried starting zipping contents of folder " + sourceLocation + " on " + startCalendar.getTime());
                    LOG.info("Tried starting zipping contents of folder " + sourceLocation + " on " + startCalendar.getTime());
                    zipFile.createZipFileFromFolder(sourceLocation, parameters, true, ZIP_SPLIT_SIZE);
                    Calendar endCalendar = Calendar.getInstance();
                    System.out.println("Contents of folder zipped to multi-part zip file "
                            + zipFile.toString() + " on " + endCalendar.getTime());
                    LOG.info("Contents of folder zipped to multi-part zip file "
                            + zipFile.toString() + " on " + endCalendar.getTime());
                } catch (Exception e) {
                    // Catch if there is a problem while creating the zip folder
                    System.out.println("Exception occurred with creating the zip file: " + e);
                    LOG.error("Exception occurred with creating the zip file: " + e);
                }
            } catch (Exception e) {
                System.out.println("Exception occurred with using the config database: " + e);
                LOG.error("Exception occurred with using the config database: " + e);
            }
        }
    }
}