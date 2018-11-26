package org.endeavourhealth.scheduler.job;

import net.lingala.zip4j.core.ZipFile;
import net.lingala.zip4j.model.ZipParameters;
import net.lingala.zip4j.util.Zip4jConstants;
import org.apache.commons.io.FileUtils;
import org.endeavourhealth.scheduler.cache.ExtractCache;
import org.endeavourhealth.scheduler.json.ExtractDefinition.ExtractConfig;
import org.endeavourhealth.scheduler.models.database.FileTransactionsEntity;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.sql.Timestamp;
import java.util.List;

public class ZipCsvFiles implements Job {

    private static final Logger LOG = LoggerFactory.getLogger(ZipCsvFiles.class);

    private static final long ZIP_SPLIT_SIZE = 10485760;

    public void execute(JobExecutionContext jobExecutionContext) {

        // System.out.println("Zipping CSV files contained in folders");
        LOG.info("Zipping CSV files contained in folders");

        int[] extractIdArray = {1};
        for (int extractId : extractIdArray) {

            try {
                // Getting the extract config from the extract table of the database
                ExtractConfig config = ExtractCache.getExtractConfig(extractId);
                // System.out.println(config.getName());
                LOG.info(config.getName());

                // Getting the folder to be zipped from the file_transactions table of the database
                List<FileTransactionsEntity> toProcess = FileTransactionsEntity.getFilesForZip(extractId);
                if (toProcess == null || toProcess.size() == 0) {
                    // System.out.println("No files for zipping");
                    LOG.info("No files for zipping");
                    return;
                }

                FileTransactionsEntity entry = toProcess.get(0);
                if (entry != null) {
                    try {

                        String sourceLocation = config.getFileLocationDetails().getSource();

                        if (!(sourceLocation.endsWith(File.separator))) {
                            sourceLocation += File.separator;
                        }

                        sourceLocation += entry.getFilename();

                        // Create the empty zip file, named from the source
                        // folder name, concatenated with the todayDateString
                        ZipFile zipFile = new ZipFile(sourceLocation + ".zip");

                        // Set the zip file parameters
                        ZipParameters parameters = new ZipParameters();
                        parameters.setCompressionMethod(Zip4jConstants.COMP_DEFLATE);
                        parameters.setCompressionLevel(Zip4jConstants.DEFLATE_LEVEL_NORMAL);
                        parameters.setIncludeRootFolder(false);

                        LOG.info("Tried starting zipping contents of folder " + sourceLocation);

                        // Create the multi-part zip file from the files in the
                        // specified folder, using the zip file parameters
                        zipFile.createZipFileFromFolder(sourceLocation, parameters,
                                true, ZIP_SPLIT_SIZE);

                        List<String> splitZipFileList = zipFile.getSplitZipFiles();
                        // System.out.println("Contents of folder zipped to multi-part zip file "
                        //        + splitZipFileList + " on " + endCalendar.getTime());
                        LOG.info("Contents of folder zipped to multi-part zip file "
                                + splitZipFileList);

                        // Add, to the file_transactions table of the database,
                        // the entries for each part of the multi-part zip file
                        for (String filePathAndName : splitZipFileList) {
                            File file = new File(filePathAndName);
                            String fileName = file.getName();
                            FileTransactionsEntity newFileTransEntityForCreation = new FileTransactionsEntity();
                            newFileTransEntityForCreation.setExtractId(extractId);
                            newFileTransEntityForCreation.setFilename(fileName);
                            newFileTransEntityForCreation.setExtractDate(entry.getExtractDate());
                            newFileTransEntityForCreation.setZipDate(new Timestamp(System.currentTimeMillis()));
                            FileTransactionsEntity.create(newFileTransEntityForCreation);
                            // System.out.println("File: " + fileName + " record created");
                            LOG.info("File: " + fileName + " record created");
                        }

                        File file = new File(sourceLocation);
                        FileUtils.deleteDirectory(file);

                        // Delete, from the file_transactions table, the entry for the folder to be zipped
                        FileTransactionsEntity.delete(entry);
                        // System.out.println("File (folder): " + sourceLocation + " record deleted");
                        LOG.info("File (folder): " + file.getName() + " record deleted");

                    } catch (Exception e) {
                        // System.out.println("Exception occurred with creating the zip file: " + e);
                        LOG.error("Exception occurred with creating the zip file: " + e);
                    }
                }
            } catch (Exception e) {
                // System.out.println("Exception occurred with using the database: " + e);
                LOG.error("Exception occurred with using the database: " + e);
            }
        }
    }
}