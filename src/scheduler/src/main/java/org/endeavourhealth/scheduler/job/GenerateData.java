package org.endeavourhealth.scheduler.job;

import org.endeavourhealth.scheduler.cache.DatasetCache;
import org.endeavourhealth.scheduler.cache.ExtractCache;
import org.endeavourhealth.scheduler.json.DatasetDefinition.DatasetCodeSet;
import org.endeavourhealth.scheduler.json.DatasetDefinition.DatasetConfig;
import org.endeavourhealth.scheduler.json.DatasetDefinition.DatasetConfigExtract;
import org.endeavourhealth.scheduler.json.DatasetDefinition.DatasetFields;
import org.endeavourhealth.scheduler.json.ExtractDefinition.ExtractConfig;
import org.endeavourhealth.scheduler.models.CustomExtracts.*;
import org.endeavourhealth.scheduler.models.database.ExtractEntity;
import org.endeavourhealth.scheduler.models.database.FileTransactionsEntity;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

public class GenerateData implements Job {

    private static final Logger LOG = LoggerFactory.getLogger(GenerateData.class);
    private boolean limitCols = false;

    public void setLimitCols(boolean limit) {
        this.limitCols = limit;
    }

    public void execute(JobExecutionContext jobExecutionContext) {

        LOG.info("Generate Data");

        try {
            List<ExtractEntity> extractsToProcess = null;
            if (jobExecutionContext.getScheduler() != null) {
                extractsToProcess = (List<ExtractEntity>) jobExecutionContext.getScheduler().getContext().get("extractsToProcess");
            } else {
                extractsToProcess = (List<ExtractEntity>) jobExecutionContext.get("extractsToProcess");
            }
            for (ExtractEntity entity : extractsToProcess) {
                processExtracts(entity.getExtractId());

                // add the row to the file_transactions table of the
                // database for each extractId set of files that is run
                FileTransactionsEntity newFileTransEntityForCreation = new FileTransactionsEntity();
                newFileTransEntityForCreation.setExtractId(entity.getExtractId());
                newFileTransEntityForCreation.setExtractDate(new Timestamp(System.currentTimeMillis()));
                Date todayDate = Calendar.getInstance().getTime();
                DateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
                String strTodayDate = dateFormat.format(todayDate);
                String extractIdAndTodayDate = entity.getExtractId() + "_" + strTodayDate;
                newFileTransEntityForCreation.setFilename(extractIdAndTodayDate);
                FileTransactionsEntity.create(newFileTransEntityForCreation);
                LOG.info("File (folder): " + extractIdAndTodayDate + " record created");
            }
        } catch (Exception e) {
            LOG.error("Error: " + e.getMessage());
            System.out.println("Error:" + e.getMessage());
        }

        LOG.info("End of Generate Data");
    }

    private void processExtracts(int extractId) throws Exception {

        ExtractEntity extractDetails = ExtractCache.getExtract(extractId);

        DatasetConfig datasetConfig = DatasetCache.getDatasetConfig(extractDetails.getDatasetId());

        if (datasetConfig.getExtract() != null) {

            Long maxTransactionId = GeneralQueries.getMaxTransactionId();

            for (DatasetConfigExtract extract : datasetConfig.getExtract()) {
                System.out.println(extract.getType());
                switch (extract.getType()) {
                    case "patient":
                        runPatientExtract(extract, extractId, "patient", extractDetails.getTransactionId(), maxTransactionId);
                        break;
                    case "medication":
                        runMedicationExtractForCodeSets(extract, extractId, "medication", extractDetails.getTransactionId(), maxTransactionId);
                        break;
                    case "observation":
                        runObservationExtractForCodeSets(extract, extractId, "observation", extractDetails.getTransactionId(), maxTransactionId);
                        break;
                    case "allergy":
                        runAllergyExtractForCodeSets(extract, extractId, "allergy", extractDetails.getTransactionId(), maxTransactionId);
                        break;
                    case "immunisation":
                        runImmunisationExtractForCodeSets(extract, extractId, "immunisation", extractDetails.getTransactionId(), maxTransactionId);
                        break;
                }
            }

            runFinaliseExtract(extractId, maxTransactionId);
        }

    }

    private void runPatientExtract(DatasetConfigExtract extractConfig, int extractId, String sectionName, Long currentTransactionId, Long maxTransactionId) throws Exception {

        List<String> fieldHeaders = extractConfig.getFields().stream().map(DatasetFields::getHeader).collect(Collectors.toList());
        List<Integer> fieldIndexes = extractConfig.getFields().stream().map(DatasetFields::getIndex).collect(Collectors.toList());

        // create the headers and the actual file
        createCSV(fieldHeaders, sectionName, extractId);

        List results;

        results = PatientExtracts.runBulkPatientExtract(extractId);
        saveToCSV(results, sectionName, fieldIndexes, extractId);
        results = PatientExtracts.runDeltaPatientExtract(extractId, currentTransactionId, maxTransactionId);
        saveToCSV(results, sectionName, fieldIndexes, extractId);

    }

    private void runObservationExtractForCodeSets(DatasetConfigExtract extractConfig, int extractId, String sectionName, Long currentTransactionId, Long maxTransactionId) throws Exception {

        List<DatasetCodeSet> codeSets = extractConfig.getCodeSets();

        List<String> fieldHeaders = extractConfig.getFields().stream().map(DatasetFields::getHeader).collect(Collectors.toList());
        List<Integer> fieldIndexes = extractConfig.getFields().stream().map(DatasetFields::getIndex).collect(Collectors.toList());

        if (codeSets != null && codeSets.size() > 0) {

            // create the headers and the actual file
            createCSV(fieldHeaders, sectionName, extractId);

            for (DatasetCodeSet codeSet : codeSets) {
                List results;
                switch (codeSet.getExtractType())
                {
                    case "all":
                        results = ObservationExtracts.runBulkObservationAllCodesQuery(extractId, codeSet.getCodeSetId());
                        saveToCSV(results, sectionName, fieldIndexes, extractId);
                        results = ObservationExtracts.runDeltaObservationAllCodesQuery(extractId, codeSet.getCodeSetId(),
                                currentTransactionId, maxTransactionId);
                        saveToCSV(results, sectionName, fieldIndexes, extractId);
                        break;
                    case "earliest_each":
                        results = ObservationExtracts.runBulkObservationEarliestEachCodesQuery(extractId, codeSet.getCodeSetId());
                        saveToCSV(results, sectionName, fieldIndexes, extractId);
                        results = ObservationExtracts.runDeltaObservationEarliestEachCodesQuery(extractId, codeSet.getCodeSetId(),
                                currentTransactionId, maxTransactionId);
                        saveToCSV(results, sectionName, fieldIndexes, extractId);
                        break;
                    case "latest_each":
                        results = ObservationExtracts.runBulkObservationLatestEachCodesQuery(extractId, codeSet.getCodeSetId());
                        saveToCSV(results, sectionName, fieldIndexes, extractId);
                        results = ObservationExtracts.runDeltaObservationLatestEachCodesQuery(extractId, codeSet.getCodeSetId(),
                                currentTransactionId, maxTransactionId);
                        saveToCSV(results, sectionName, fieldIndexes, extractId);
                        break;
                    case "earliest":
                        results = ObservationExtracts.runBulkObservationEarliestCodesQuery(extractId, codeSet.getCodeSetId());
                        saveToCSV(results, sectionName, fieldIndexes, extractId);
                        results = ObservationExtracts.runDeltaObservationEarliestCodesQuery(extractId, codeSet.getCodeSetId(),
                                currentTransactionId, maxTransactionId);
                        saveToCSV(results, sectionName, fieldIndexes, extractId);
                        break;
                    case "latest":
                        results = ObservationExtracts.runBulkObservationLatestCodesQuery(extractId, codeSet.getCodeSetId());
                        saveToCSV(results, sectionName, fieldIndexes, extractId);
                        results = ObservationExtracts.runDeltaObservationLatestCodesQuery(extractId, codeSet.getCodeSetId(),
                                currentTransactionId, maxTransactionId);
                        saveToCSV(results, sectionName, fieldIndexes, extractId);
                        break;
                }
            }
        }
    }

    private void runAllergyExtractForCodeSets(DatasetConfigExtract extractConfig, int extractId, String sectionName, Long currentTransactionId, Long maxTransactionId) throws Exception {

        List<DatasetCodeSet> codeSets = extractConfig.getCodeSets();

        List<String> fieldHeaders = extractConfig.getFields().stream().map(DatasetFields::getHeader).collect(Collectors.toList());
        List<Integer> fieldIndexes = extractConfig.getFields().stream().map(DatasetFields::getIndex).collect(Collectors.toList());

        if (codeSets != null && codeSets.size() > 0) {

            // create the headers and the actual file
            createCSV(fieldHeaders, sectionName, extractId);

            for (DatasetCodeSet codeSet : codeSets) {
                List results;
                switch (codeSet.getExtractType())
                {
                    case "all":
                        results = AllergyExtracts.runBulkAllergyAllCodesQuery(extractId, codeSet.getCodeSetId());
                        saveToCSV(results, sectionName, fieldIndexes, extractId);
                        results = AllergyExtracts.runDeltaAllergyAllCodesQuery(extractId, codeSet.getCodeSetId(),
                                currentTransactionId, maxTransactionId);
                        saveToCSV(results, sectionName, fieldIndexes, extractId);
                        break;
                    case "earliest_each":
                        results = AllergyExtracts.runBulkAllergyEarliestEachCodesQuery(extractId, codeSet.getCodeSetId());
                        saveToCSV(results, sectionName, fieldIndexes, extractId);
                        results = AllergyExtracts.runDeltaAllergyEarliestEachCodesQuery(extractId, codeSet.getCodeSetId(),
                                currentTransactionId, maxTransactionId);
                        saveToCSV(results, sectionName, fieldIndexes, extractId);
                        break;
                    case "latest_each":
                        results = AllergyExtracts.runBulkAllergyLatestEachCodesQuery(extractId, codeSet.getCodeSetId());
                        saveToCSV(results, sectionName, fieldIndexes, extractId);
                        results = AllergyExtracts.runDeltaAllergyLatestEachCodesQuery(extractId, codeSet.getCodeSetId(),
                                currentTransactionId, maxTransactionId);
                        saveToCSV(results, sectionName, fieldIndexes, extractId);
                        break;
                    case "earliest":
                        results = AllergyExtracts.runBulkAllergyEarliestCodesQuery(extractId, codeSet.getCodeSetId());
                        saveToCSV(results, sectionName, fieldIndexes, extractId);
                        results = AllergyExtracts.runDeltaAllergyEarliestCodesQuery(extractId, codeSet.getCodeSetId(),
                                currentTransactionId, maxTransactionId);
                        saveToCSV(results, sectionName, fieldIndexes, extractId);
                        break;
                    case "latest":
                        results = AllergyExtracts.runBulkAllergyLatestCodesQuery(extractId, codeSet.getCodeSetId());
                        saveToCSV(results, sectionName, fieldIndexes, extractId);
                        results = AllergyExtracts.runDeltaAllergyLatestCodesQuery(extractId, codeSet.getCodeSetId(),
                                currentTransactionId, maxTransactionId);
                        saveToCSV(results, sectionName, fieldIndexes, extractId);
                        break;
                }
            }
        }
    }

    private void runImmunisationExtractForCodeSets(DatasetConfigExtract extractConfig, int extractId, String sectionName, Long currentTransactionId, Long maxTransactionId) throws Exception {

        List<DatasetCodeSet> codeSets = extractConfig.getCodeSets();

        List<String> fieldHeaders = extractConfig.getFields().stream().map(DatasetFields::getHeader).collect(Collectors.toList());
        List<Integer> fieldIndexes = extractConfig.getFields().stream().map(DatasetFields::getIndex).collect(Collectors.toList());

        if (codeSets != null && codeSets.size() > 0) {

            // create the headers and the actual file
            createCSV(fieldHeaders, sectionName, extractId);

            for (DatasetCodeSet codeSet : codeSets) {
                List results;
                switch (codeSet.getExtractType())
                {
                    case "all":
                        results = ImmunisationExtracts.runBulkImmunisationAllCodesQuery(extractId, codeSet.getCodeSetId());
                        saveToCSV(results, sectionName, fieldIndexes, extractId);
                        results = ImmunisationExtracts.runDeltaImmunisationAllCodesQuery(extractId, codeSet.getCodeSetId(),
                                currentTransactionId, maxTransactionId);
                        saveToCSV(results, sectionName, fieldIndexes, extractId);
                        break;
                    case "earliest_each":
                        results = ImmunisationExtracts.runBulkImmunisationEarliestEachCodesQuery(extractId, codeSet.getCodeSetId());
                        saveToCSV(results, sectionName, fieldIndexes, extractId);
                        results = ImmunisationExtracts.runDeltaImmunisationEarliestEachCodesQuery(extractId, codeSet.getCodeSetId(),
                                currentTransactionId, maxTransactionId);
                        saveToCSV(results, sectionName, fieldIndexes, extractId);
                        break;
                    case "latest_each":
                        results = ImmunisationExtracts.runBulkImmunisationLatestEachCodesQuery(extractId, codeSet.getCodeSetId());
                        saveToCSV(results, sectionName, fieldIndexes, extractId);
                        results = ImmunisationExtracts.runDeltaImmunisationLatestEachCodesQuery(extractId, codeSet.getCodeSetId(),
                                currentTransactionId, maxTransactionId);
                        saveToCSV(results, sectionName, fieldIndexes, extractId);
                        break;
                    case "earliest":
                        results = ImmunisationExtracts.runBulkImmunisationEarliestCodesQuery(extractId, codeSet.getCodeSetId());
                        saveToCSV(results, sectionName, fieldIndexes, extractId);
                        results = ImmunisationExtracts.runDeltaImmunisationEarliestCodesQuery(extractId, codeSet.getCodeSetId(),
                                currentTransactionId, maxTransactionId);
                        saveToCSV(results, sectionName, fieldIndexes, extractId);
                        break;
                    case "latest":
                        results = ImmunisationExtracts.runBulkImmunisationLatestCodesQuery(extractId, codeSet.getCodeSetId());
                        saveToCSV(results, sectionName, fieldIndexes, extractId);
                        results = ImmunisationExtracts.runDeltaImmunisationLatestCodesQuery(extractId, codeSet.getCodeSetId(),
                                currentTransactionId, maxTransactionId);
                        saveToCSV(results, sectionName, fieldIndexes, extractId);
                        break;
                }
            }
        }
    }

    private void runMedicationExtractForCodeSets(DatasetConfigExtract extractConfig, int extractId, String sectionName, Long currentTransactionId, Long maxTransactionId) throws Exception {

        List<DatasetCodeSet> codeSets = extractConfig.getCodeSets();

        List<String> fieldHeaders = extractConfig.getFields().stream().map(DatasetFields::getHeader).collect(Collectors.toList());
        List<Integer> fieldIndexes = extractConfig.getFields().stream().map(DatasetFields::getIndex).collect(Collectors.toList());

        if (codeSets != null && codeSets.size() > 0) {

            // create the headers and the actual file
            createCSV(fieldHeaders, sectionName, extractId);

            for (DatasetCodeSet codeSet : codeSets) {
                List results;
                switch (codeSet.getExtractType())
                {
                    case "all":
                        results = MedicationExtracts.runBulkMedicationAllCodesQuery(extractId, codeSet.getCodeSetId());
                        saveToCSV(results, sectionName, fieldIndexes, extractId);
                        results = MedicationExtracts.runDeltaMedicationAllCodesQuery(extractId, codeSet.getCodeSetId(),
                                currentTransactionId, maxTransactionId);
                        saveToCSV(results, sectionName, fieldIndexes, extractId);
                        break;
                    case "earliest_each":
                        results = MedicationExtracts.runBulkMedicationEarliestEachCodesQuery(extractId, codeSet.getCodeSetId());
                        saveToCSV(results, sectionName, fieldIndexes, extractId);
                        results = MedicationExtracts.runDeltaMedicationEarliestEachCodesQuery(extractId, codeSet.getCodeSetId(),
                                currentTransactionId, maxTransactionId);
                        saveToCSV(results, sectionName, fieldIndexes, extractId);
                        break;
                    case "latest_each":
                        results = MedicationExtracts.runBulkMedicationLatestEachCodesQuery(extractId, codeSet.getCodeSetId());
                        saveToCSV(results, sectionName, fieldIndexes, extractId);
                        results = MedicationExtracts.runDeltaMedicationLatestEachCodesQuery(extractId, codeSet.getCodeSetId(),
                                currentTransactionId, maxTransactionId);
                        saveToCSV(results, sectionName, fieldIndexes, extractId);
                        break;
                    case "earliest":
                        results = MedicationExtracts.runBulkMedicationEarliestCodesQuery(extractId, codeSet.getCodeSetId());
                        saveToCSV(results, sectionName, fieldIndexes, extractId);
                        results = MedicationExtracts.runDeltaMedicationEarliestCodesQuery(extractId, codeSet.getCodeSetId(),
                                currentTransactionId, maxTransactionId);
                        saveToCSV(results, sectionName, fieldIndexes, extractId);
                        break;
                    case "latest":
                        results = MedicationExtracts.runBulkMedicationLatestCodesQuery(extractId, codeSet.getCodeSetId());
                        saveToCSV(results, sectionName, fieldIndexes, extractId);
                        results = MedicationExtracts.runDeltaMedicationLatestCodesQuery(extractId, codeSet.getCodeSetId(),
                                currentTransactionId, maxTransactionId);
                        saveToCSV(results, sectionName, fieldIndexes, extractId);
                        break;
                }
            }
        }
    }

    private void runFinaliseExtract(int extractId, Long maxTransactionId) throws Exception {

        GeneralQueries.setBulkedStatus(extractId);
        GeneralQueries.setTransactionId(extractId, maxTransactionId);
    }

    private void createCSV(List<String> fieldHeaders, String tableName, int extractId) throws Exception {

        ExtractConfig config = ExtractCache.getExtractConfig(extractId);

        // creates directory named by sourceLocation pathname, and any necessary non-existent
        // parent directories, so useful for first run of any new extract added to the database
        String sourceLocation = config.getFileLocationDetails().getSource();
        if (!(sourceLocation.endsWith(File.separator))) {
            sourceLocation += File.separator;
        }
        File sourceLocDir = new File(sourceLocation);
        if (!(sourceLocDir.exists())) {
            sourceLocDir.mkdirs();
        }

        // creates directory named by housekeepLocation pathname, only creating that directory,
        // as the rest of the folder structure, within which it sits, has been created above
        String housekeepLocation = config.getFileLocationDetails().getHousekeep();
        if (!(housekeepLocation.endsWith(File.separator))) {
            housekeepLocation += File.separator;
        }
        File houseLocDir = new File(housekeepLocation);
        if (!(houseLocDir.exists())) {
            houseLocDir.mkdir();
        }

        // creates directory for today's csv files, if not already created
        Date todayDate = Calendar.getInstance().getTime();
        DateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
        String strTodayDate = dateFormat.format(todayDate);
        String extractIdAndTodayDate = extractId + "_" + strTodayDate;
        String strTodayDir = sourceLocation + extractIdAndTodayDate;
        if (!(strTodayDir.endsWith(File.separator))) {
            strTodayDir += File.separator;
        }
        File todayDir = new File(strTodayDir);
        if (!(todayDir.exists())) {
            todayDir.mkdir();
        }

        String filename = strTodayDir + extractIdAndTodayDate + "_" + tableName + ".csv";

        FileWriter fw = new FileWriter(filename);
        try {
            int counter = 0;
            for (String field : fieldHeaders) {
                fw.append("\"" + field + "\"");
                counter ++;
                if (counter < fieldHeaders.size()){
                    fw.append(',');
                }
            }
            fw.append(System.getProperty("line.separator"));
        } finally {
            fw.flush();
            fw.close();
            System.out.println("Headers saved in " + tableName + ".csv");
        }
    }

    private void saveToCSV(List<Object[]> results,  String tableName, List<Integer> fieldIndexes, int extractId) throws Exception {

        ExtractConfig config = ExtractCache.getExtractConfig(extractId);

        String sourceLocation = config.getFileLocationDetails().getSource();

        Date todayDate = Calendar.getInstance().getTime();
        DateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
        String strTodayDate = dateFormat.format(todayDate);
        String extractIdAndTodayDate = extractId + "_" + strTodayDate;

        String strTodayDir = sourceLocation + extractIdAndTodayDate;
        if (!(strTodayDir.endsWith(File.separator))) {
            strTodayDir += File.separator;
        }

        String filename = strTodayDir + extractIdAndTodayDate + "_" + tableName + ".csv";

        FileWriter fw = new FileWriter(filename, true);
        try {
            for (Object[] result : results) {
                int counter = 0;
                for (Integer idx : fieldIndexes) {
                    if (result[idx] != null) {
                        fw.append("\"" + result[idx].toString() + "\"");
                    } else {
                        fw.append("\"\"");
                    }
                    counter ++;
                    if (counter < fieldIndexes.size()){
                        fw.append(',');
                    }
                }
                fw.append(System.getProperty("line.separator"));
            }
        } finally {
            fw.flush();
            fw.close();
            System.out.println("data added to " + tableName);
        }
    }
}