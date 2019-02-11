package org.endeavourhealth.scheduler.job;

import net.lingala.zip4j.core.ZipFile;
import net.lingala.zip4j.model.ZipParameters;
import net.lingala.zip4j.util.Zip4jConstants;
import org.apache.commons.io.FileUtils;
import org.endeavourhealth.scheduler.Main;
import org.endeavourhealth.scheduler.cache.ExtractCache;
import org.endeavourhealth.scheduler.json.ExtractDefinition.ExtractConfig;
import org.endeavourhealth.scheduler.models.database.ExtractEntity;
import org.endeavourhealth.scheduler.models.database.FileTransactionsEntity;
import org.endeavourhealth.scheduler.util.JobUtil;
import org.quartz.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.sql.Timestamp;
import java.util.List;

public class ZipCsvFiles implements Job {

    private static final Logger LOG = LoggerFactory.getLogger(ZipCsvFiles.class);

    private static final long ZIP_SPLIT_SIZE = 10485760;

    @Override
    public void execute(JobExecutionContext jobExecutionContext) {

        List<ExtractEntity> extractsToProcess = null;
        try {
            if (jobExecutionContext.getScheduler() != null) {
                extractsToProcess = (List<ExtractEntity>) jobExecutionContext.getScheduler().getContext().get("extractsToProcess");
                if (JobUtil.isJobRunning(jobExecutionContext,
                        new String[] {
                                Main.ENCRYPT_FILES_JOB,
                                Main.SFTP_FILES_JOB,
                                Main.HOUSEKEEP_FILES_JOB},
                        Main.FILE_JOB_GROUP)) {

                    LOG.info("Conflicting job is still running");
                    return;
                }
            } else {
                extractsToProcess = (List<ExtractEntity>) jobExecutionContext.get("extractsToProcess");
            }
        } catch (Exception e) {
            LOG.error("Unknown error encountered in zip handling. " + e.getMessage());
        }
        LOG.info("Beginning of zipping CSV files");

        Main main = Main.getInstance();
        for (ExtractEntity entity : extractsToProcess) {

            LOG.info("Extract ID: " + entity.getExtractId());

            try {
                // Getting the extract config from the extract table of the database
                ExtractConfig config = ExtractCache.getExtractConfig(entity.getExtractId());
                // LOG.info(config.getName());

                // Getting the folder to be zipped from the file_transactions table of the database
                List<FileTransactionsEntity> toProcess = FileTransactionsEntity.getFilesForZip(entity.getExtractId());
                if (toProcess == null || toProcess.size() == 0) {
                    LOG.info("No files for zipping");
                } else {
                    FileTransactionsEntity entry = toProcess.get(0);
                    if (entry != null) {
                        try {
                            // The below takes the contents of a specified folder location (i.e. all its files)
                            // and creates a multi-part zip file that is put into a different location

                            String sourceLocation = config.getFileLocationDetails().getSource();

                            if (!(sourceLocation.endsWith(File.separator))) {
                                sourceLocation += File.separator;
                            }

                            sourceLocation += entry.getFilename();

                            // Create the empty zip file
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
                            LOG.info("Contents of folder zipped to multi-part zip file "
                                    + splitZipFileList);

                            try {
                            // Add, to the file_transactions table of the database,
                            // the entries for each part of the multi-part zip file
                            for (String filePathAndName : splitZipFileList) {
                                File file = new File(filePathAndName);
                                String fileName = file.getName();
                                FileTransactionsEntity newFileTransEntityForCreation = new FileTransactionsEntity();
                                newFileTransEntityForCreation.setExtractId(entity.getExtractId());
                                newFileTransEntityForCreation.setFilename(fileName);
                                newFileTransEntityForCreation.setExtractDate(entry.getExtractDate());
                                newFileTransEntityForCreation.setZipDate(new Timestamp(System.currentTimeMillis()));
                                FileTransactionsEntity.create(newFileTransEntityForCreation);
                                LOG.info("File: " + fileName + " record created");
                                main.endJob(Main.ZIP_FILES_JOB, main.incrememtZipProcessed());
                            }
                            } catch (Exception e) {
                                LOG.error("Exception occurred with using the database: " + e);
                            }

                            File file = new File(sourceLocation);
                            FileUtils.deleteDirectory(file);
                            LOG.info(sourceLocation + " folder deleted");
                            try {
                            // Delete, from the file_transactions table, the entry for the folder to be zipped
                            FileTransactionsEntity.delete(entry);
                            LOG.info("File (folder): " + file.getName() + " record deleted");
                            } catch (Exception e) {
                                LOG.error("Exception occurred with using the database: " + e);
                            }

                        } catch (Exception e) {
                            LOG.error("Exception occurred with creating the zip file: " + e);
                            main.errorEncountered(main.incrememtErrorCount());
                        }
                    }
                }
            } catch (Exception e) {
                LOG.error("Exception occurred with using the database: " + e);
            }
        }
        LOG.info("End of zipping CSV files");
    }
}