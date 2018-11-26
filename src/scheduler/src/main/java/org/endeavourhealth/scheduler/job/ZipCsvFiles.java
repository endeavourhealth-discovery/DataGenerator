package org.endeavourhealth.scheduler.job;

        import net.lingala.zip4j.core.ZipFile;
        import net.lingala.zip4j.model.ZipParameters;
        import net.lingala.zip4j.util.Zip4jConstants;
        import org.endeavourhealth.scheduler.cache.ExtractCache;
        import org.endeavourhealth.scheduler.json.ExtractDefinition.ExtractConfig;
        import org.endeavourhealth.scheduler.models.database.FileTransactionsEntity;
        import org.quartz.Job;
        import org.quartz.JobExecutionContext;
        import org.slf4j.Logger;
        import org.slf4j.LoggerFactory;

        import java.io.File;
        import java.sql.Timestamp;
        import java.util.Calendar;
        import java.util.List;

public class ZipCsvFiles implements Job {

    private static final Logger LOG = LoggerFactory.getLogger(ZipCsvFiles.class);

    private static final long ZIP_SPLIT_SIZE = 2621440; // 5242880 // 10485760

    public void execute(JobExecutionContext jobExecutionContext) {

        // System.out.println("Zipping CSV files contained in folders");
        LOG.info("Zipping CSV files contained in folders");

        int[] extractIdArray = {2};
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

                for (FileTransactionsEntity entry : toProcess) {
                    try {
                        // This turns a single file into a multi-part zip file

                        // String sourcePath = "C:/ziplocation/largefiletest1m32mb.csv";
                        // File sourceFile = new File(sourcePath);
                        // File destinationZipFolder = new File (sourceFile.getParent(),
                        // sourceFile.getName() + ".zip");
                        // ZipFile zip = new ZipFile(destinationZipFolder);
                        // zip.createZipFile(sourceFile, parameters, true, ZIP_SPLIT_SIZE);

                        // The below takes the contents of a specified folder location (i.e. all its files)
                        // and creates a multi-part zip file in the same place: original files not deleted

                        String sourceLocation = config.getFileLocationDetails().getSource();

                        if (!(sourceLocation.endsWith(File.separator))) {
                            sourceLocation += File.separator;
                        }

                        File file1 = new File(sourceLocation);
                        String zipFilename = file1.getName(); // or .getParent() if the subfolder is called Source
                        // and the parent folder is the name of the Product

                        // Create a string in CCYYMMDD format for today's date
                        Calendar calendar = Calendar.getInstance();
                        Integer year = calendar.get(calendar.YEAR);
                        Integer month = calendar.get(calendar.MONTH);
                        month = month + 1;
                        String monthString = null;
                        if (month < 10) {monthString = "0" + month;}
                        else {monthString = month.toString();}
                        Integer day = calendar.get(calendar.DAY_OF_MONTH);
                        String dayString = null;
                        if (day < 10) {dayString = "0" + day;}
                        else {dayString = day.toString();}
                        String todayDateString = year + monthString + dayString;

                        // Create the empty zip file, named from the source
                        // folder name, concatenated with the todayDateString
                        ZipFile zipFile = new ZipFile(sourceLocation +
                                zipFilename + "_" + todayDateString + ".zip");

                        // Set the zip file parameters
                        ZipParameters parameters = new ZipParameters();
                        parameters.setCompressionMethod(Zip4jConstants.COMP_DEFLATE);
                        parameters.setCompressionLevel(Zip4jConstants.DEFLATE_LEVEL_NORMAL);
                        parameters.setIncludeRootFolder(false);

                        Calendar startCalendar = Calendar.getInstance();
                        // System.out.println("Tried starting zipping contents of folder " + sourceLocation
                        //        + " on " + startCalendar.getTime());
                        LOG.info("Tried starting zipping contents of folder " + sourceLocation
                                + " on " + startCalendar.getTime());

                        // Create the multi-part zip file from the files in the
                        // specified folder, using the zip file parameters
                        zipFile.createZipFileFromFolder(sourceLocation, parameters,
                                true, ZIP_SPLIT_SIZE);

                        Calendar endCalendar = Calendar.getInstance();
                        List<String> splitZipFileList = zipFile.getSplitZipFiles();
                        // System.out.println("Contents of folder zipped to multi-part zip file "
                        //        + splitZipFileList + " on " + endCalendar.getTime());
                        LOG.info("Contents of folder zipped to multi-part zip file "
                                + splitZipFileList + " on " + endCalendar.getTime());

                        // Add, to the file_transactions table of the database,
                        // the entries for each part of the multi-part zip file
                        for (String filePathAndName : splitZipFileList) {
                            File file2 = new File(filePathAndName);
                            String fileName = file2.getName();
                            FileTransactionsEntity newFileTransEntityForCreation = new FileTransactionsEntity();
                            newFileTransEntityForCreation.setExtractId(extractId);
                            newFileTransEntityForCreation.setFilename(fileName);
                            newFileTransEntityForCreation.setExtractDate(entry.getExtractDate());
                            newFileTransEntityForCreation.setZipDate(new Timestamp(System.currentTimeMillis()));
                            FileTransactionsEntity.create(newFileTransEntityForCreation);
                            // System.out.println("File: " + fileName + " record created");
                            LOG.info("File: " + fileName + " record created");
                        }

                        // Delete the .csv files from the specified folder location
                        File file3 = new File(sourceLocation);
                        String[] fileList = file3.list();
                        for (String filename : fileList){
                            if (filename.contains(".csv")){
                                File fileForDeletion = new File(sourceLocation + filename);
                                fileForDeletion.delete();
                                // System.out.println(filename + " has been deleted");
                                LOG.info(filename + " has been deleted");
                            }
                        }

                        // Delete, from the file_transactions table, the entry for the folder to be zipped
                        // FileTransactionsEntity.delete(entry);
                        // System.out.println("File (folder): " + sourceLocation + " record deleted");
                        // LOG.info("File (folder): " + sourceLocation + " record deleted");

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