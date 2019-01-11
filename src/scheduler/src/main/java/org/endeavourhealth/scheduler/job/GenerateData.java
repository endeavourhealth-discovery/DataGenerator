package org.endeavourhealth.scheduler.job;

import org.endeavourhealth.scheduler.Main;
import org.endeavourhealth.scheduler.cache.DatasetCache;
import org.endeavourhealth.scheduler.cache.ExtractCache;
import org.endeavourhealth.scheduler.json.DatasetDefinition.DatasetCodeSet;
import org.endeavourhealth.scheduler.json.DatasetDefinition.DatasetConfig;
import org.endeavourhealth.scheduler.json.DatasetDefinition.DatasetConfigExtract;
import org.endeavourhealth.scheduler.json.DatasetDefinition.DatasetFields;
import org.endeavourhealth.scheduler.json.ExtractDefinition.ExtractConfig;
import org.endeavourhealth.scheduler.models.CustomExtracts.*;
import org.endeavourhealth.scheduler.models.database.ExportedIdsEntity;
import org.endeavourhealth.scheduler.models.database.ExtractEntity;
import org.endeavourhealth.scheduler.models.database.FileTransactionsEntity;
import org.quartz.DisallowConcurrentExecution;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.PersistJobDataAfterExecution;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

@DisallowConcurrentExecution
@PersistJobDataAfterExecution
public class GenerateData implements Job {

    private static final Logger LOG = LoggerFactory.getLogger(GenerateData.class);
    private boolean limitCols = false;
    private static final int PAGE_SIZE = 200000;
    private List<Long> currentResultsIds = new ArrayList<>();

    public void setLimitCols(boolean limit) {
        this.limitCols = limit;
    }

    @Override
    public void execute(JobExecutionContext jobExecutionContext) {

        List<ExtractEntity> extractsToProcess = null;
        try {
            if (jobExecutionContext.getScheduler() != null) {
                extractsToProcess = (List<ExtractEntity>) jobExecutionContext.getScheduler().getContext().get("extractsToProcess");
            } else {
                extractsToProcess = (List<ExtractEntity>) jobExecutionContext.get("extractsToProcess");
            }
        } catch (Exception e) {
            LOG.error("Unknown error encountered in generating data handling. " + e.getMessage());
        }
        LOG.info("Beginning of generating data extracts to CSV files");

        for (ExtractEntity entity : extractsToProcess) {

            int extractId = entity.getExtractId();
            LOG.info("Extract ID: " + extractId);

            try {
                this.createSourceAndHousekeepDirectories(extractId);
                String sourceLocation = this.createSourceDirectoryString(extractId);
                String extractIdAndTodayDate = this.createExtractIdAndTodayDateString(extractId);
                this.createTodayDirectory(sourceLocation, extractIdAndTodayDate);
                this.processExtracts(extractId);
                try {
                    // add the row to the file_transactions table of the
                    // database for each extractId set of files that is run
                    FileTransactionsEntity newFileTransEntityForCreation = new FileTransactionsEntity();
                    newFileTransEntityForCreation.setExtractId(extractId);
                    newFileTransEntityForCreation.setExtractDate(new Timestamp(System.currentTimeMillis()));
                    newFileTransEntityForCreation.setFilename(extractIdAndTodayDate);
                    FileTransactionsEntity.create(newFileTransEntityForCreation);
                    LOG.info("File (folder): " + extractIdAndTodayDate + " record created");
                } catch (Exception e) {
                    LOG.error("Exception occurred with using the database: " + e);
                }
            } catch (Exception e) {
                LOG.error("Exception occurred with generating data extracts: " + e);
                e.printStackTrace();
            }
        }
        Main.generateFilesDone = true;
        LOG.info("End of generating data extracts to CSV files");
    }

    private void processExtracts(int extractId) throws Exception {

        ExtractEntity extractDetails = ExtractCache.getExtract(extractId);

        DatasetConfig datasetConfig = DatasetCache.getDatasetConfig(extractDetails.getDatasetId());

        Long currentTransactionId = extractDetails.getTransactionId();

        if (datasetConfig.getExtract() != null) {

            Long maxTransactionId = GeneralQueries.getMaxTransactionId();

            for (DatasetConfigExtract extract : datasetConfig.getExtract()) {
                // System.out.println(extract.getType());
                switch (extract.getType()) {
                    case "patient":
                        runPatientExtract(extract, extractId, "patient", currentTransactionId, maxTransactionId);
                        if (currentTransactionId > 0) {
                            runDeletionsExtract(extractId, "patient", currentTransactionId, maxTransactionId);
                        }
                        LOG.info("Finished writing data rows to " + extract.getType() + " CSV file");
                        break;
                    case "medication":
                        runMedicationExtractForCodeSets(extract, extractId, "medication", currentTransactionId, maxTransactionId);
                        if (currentTransactionId > 0) {
                            runDeletionsExtract(extractId, "medication", currentTransactionId, maxTransactionId);
                        }
                        LOG.info("Finished writing data rows to " + extract.getType() + " CSV file");
                        break;
                    case "observation":
                        runObservationExtractForCodeSets(extract, extractId, "observation", currentTransactionId, maxTransactionId);
                        if (currentTransactionId > 0) {
                            runDeletionsExtract(extractId, "observation", currentTransactionId, maxTransactionId);
                        }
                        LOG.info("Finished writing data rows to " + extract.getType() + " CSV file");
                        break;
                    case "allergy":
                        runAllergyExtractForCodeSets(extract, extractId, "allergy", currentTransactionId, maxTransactionId);
                        if (currentTransactionId > 0) {
                            runDeletionsExtract(extractId, "allergy", currentTransactionId, maxTransactionId);
                        }
                        LOG.info("Finished writing data rows to " + extract.getType() + " CSV file");
                        break;
                    case "immunisation":
                        runImmunisationExtractForCodeSets(extract, extractId, "immunisation", currentTransactionId, maxTransactionId);
                        if (currentTransactionId > 0) {
                            runDeletionsExtract(extractId, "immunisation", currentTransactionId, maxTransactionId);
                        }
                        LOG.info("Finished writing data rows to " + extract.getType() + " CSV file");
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
        int page = 1;
        results = PatientExtracts.runBulkPatientExtract(extractId, page++, PAGE_SIZE);
        while (results.size() > 0) {
            saveToCSV(results, sectionName, fieldIndexes, extractId, true);
            results = PatientExtracts.runBulkPatientExtract(extractId, page++, PAGE_SIZE);
        }
        if (currentTransactionId > 0) {
            results = PatientExtracts.runDeltaPatientExtract(extractId, currentTransactionId, maxTransactionId);
            saveToCSV(results, sectionName, fieldIndexes, extractId, true);
        }
    }

    private void runDeletionsExtract(int extractId, String sectionName, Long currentTransactionId, Long maxTransactionId) throws Exception {

        int tableId = getTableId(sectionName);

        sectionName += "_deletions";
        List<String> fieldHeaders = new ArrayList<>();
        fieldHeaders.add("id");
        List<Integer> fieldIndexes = new ArrayList<>();
        fieldIndexes.add(0);

        // create the headers and the actual file
        createCSV(fieldHeaders, sectionName, extractId);

        List results;

        results = GeneralQueries.getDeletionsForTable(tableId, extractId, currentTransactionId, maxTransactionId);
        saveToCSV(results, sectionName, fieldIndexes, extractId, false);
        LOG.info("Processed deletions for " + sectionName);
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
                List resultsToRemove;
                int page;
                switch (codeSet.getExtractType()) {
                    case "all":
                        page = 1;
                        results = ObservationExtracts.runBulkObservationAllCodesQuery(extractId, codeSet.getCodeSetId(), page++, PAGE_SIZE);
                        while (results.size() > 0) {
                            resultsToRemove = this.removeDuplicateResultsBetweenCodeSets(results);
                            if (!(resultsToRemove.isEmpty())) {
                                results.removeAll(resultsToRemove);
                            }
                            saveToCSV(results, sectionName, fieldIndexes, extractId, true);
                            results = ObservationExtracts.runBulkObservationAllCodesQuery(extractId, codeSet.getCodeSetId(), page++, PAGE_SIZE);
                        }
                        if (currentTransactionId > 0) {
                            results = ObservationExtracts.runDeltaObservationAllCodesQuery(extractId, codeSet.getCodeSetId(),
                                    currentTransactionId, maxTransactionId);
                            resultsToRemove = this.removeDuplicateResultsBetweenCodeSets(results);
                            if (!(resultsToRemove.isEmpty())) {
                                results.removeAll(resultsToRemove);
                            }
                            saveToCSV(results, sectionName, fieldIndexes, extractId, true);
                        }
                        break;

                    case "earliest_each":
                        page = 1;
                        results = ObservationExtracts.runBulkObservationEarliestEachCodesQuery(extractId, codeSet.getCodeSetId(), page++, PAGE_SIZE);
                        while (results.size() > 0) {
                            resultsToRemove = this.removeDuplicateResultsBetweenCodeSets(results);
                            if (!(resultsToRemove.isEmpty())) {
                                results.removeAll(resultsToRemove);
                            }
                            saveToCSV(results, sectionName, fieldIndexes, extractId, true);
                            results = ObservationExtracts.runBulkObservationEarliestEachCodesQuery(extractId, codeSet.getCodeSetId(), page++, PAGE_SIZE);
                        }

                        if (currentTransactionId > 0) {
                            results = ObservationExtracts.runDeltaObservationEarliestEachCodesQuery(extractId, codeSet.getCodeSetId(),
                                    currentTransactionId, maxTransactionId);
                            resultsToRemove = this.removeDuplicateResultsBetweenCodeSets(results);
                            if (!(resultsToRemove.isEmpty())) {
                                results.removeAll(resultsToRemove);
                            }
                            saveToCSV(results, sectionName, fieldIndexes, extractId, true);
                        }
                        break;

                    case "latest_each":
                        page = 1;
                        results = ObservationExtracts.runBulkObservationLatestEachCodesQuery(extractId, codeSet.getCodeSetId(), page++, PAGE_SIZE);
                        while (results.size() > 0) {
                            resultsToRemove = this.removeDuplicateResultsBetweenCodeSets(results);
                            if (!(resultsToRemove.isEmpty())) {
                                results.removeAll(resultsToRemove);
                            }
                            saveToCSV(results, sectionName, fieldIndexes, extractId, true);
                            results = ObservationExtracts.runBulkObservationLatestEachCodesQuery(extractId, codeSet.getCodeSetId(), page++, PAGE_SIZE);
                        }
                        if (currentTransactionId > 0) {
                            results = ObservationExtracts.runDeltaObservationLatestEachCodesQuery(extractId, codeSet.getCodeSetId(),
                                    currentTransactionId, maxTransactionId);
                            resultsToRemove = this.removeDuplicateResultsBetweenCodeSets(results);
                            if (!(resultsToRemove.isEmpty())) {
                                results.removeAll(resultsToRemove);
                            }
                            saveToCSV(results, sectionName, fieldIndexes, extractId, true);
                        }
                        break;

                    case "earliest":
                        page = 1;
                        results = ObservationExtracts.runBulkObservationEarliestCodesQuery(extractId, codeSet.getCodeSetId(), page++, PAGE_SIZE);
                        while (results.size() > 0) {
                            resultsToRemove = this.removeDuplicateResultsBetweenCodeSets(results);
                            if (!(resultsToRemove.isEmpty())) {
                                results.removeAll(resultsToRemove);
                            }
                            saveToCSV(results, sectionName, fieldIndexes, extractId, true);
                            results = ObservationExtracts.runBulkObservationEarliestCodesQuery(extractId, codeSet.getCodeSetId(), page++, PAGE_SIZE);
                        }
                        if (currentTransactionId > 0) {
                            results = ObservationExtracts.runDeltaObservationEarliestCodesQuery(extractId, codeSet.getCodeSetId(),
                                    currentTransactionId, maxTransactionId);
                            resultsToRemove = this.removeDuplicateResultsBetweenCodeSets(results);
                            if (!(resultsToRemove.isEmpty())) {
                                results.removeAll(resultsToRemove);
                            }
                            saveToCSV(results, sectionName, fieldIndexes, extractId, true);
                        }
                        break;

                    case "latest":
                        page = 1;
                        results = ObservationExtracts.runBulkObservationLatestCodesQuery(extractId, codeSet.getCodeSetId(), page++, PAGE_SIZE);
                        while (results.size() > 0) {
                            resultsToRemove = this.removeDuplicateResultsBetweenCodeSets(results);
                            if (!(resultsToRemove.isEmpty())) {
                                results.removeAll(resultsToRemove);
                            }
                            saveToCSV(results, sectionName, fieldIndexes, extractId, true);
                            results = ObservationExtracts.runBulkObservationLatestCodesQuery(extractId, codeSet.getCodeSetId(), page++, PAGE_SIZE);
                        }
                        if (currentTransactionId > 0) {
                            results = ObservationExtracts.runDeltaObservationLatestCodesQuery(extractId, codeSet.getCodeSetId(),
                                    currentTransactionId, maxTransactionId);
                            resultsToRemove = this.removeDuplicateResultsBetweenCodeSets(results);
                            if (!(resultsToRemove.isEmpty())) {
                                results.removeAll(resultsToRemove);
                            }
                            saveToCSV(results, sectionName, fieldIndexes, extractId, true);
                        }
                        break;
                }
            }
            currentResultsIds.clear();
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
                List resultsToRemove;
                int page;
                switch (codeSet.getExtractType()) {
                    case "all":
                        page = 1;
                        results = AllergyExtracts.runBulkAllergyAllCodesQuery(extractId, codeSet.getCodeSetId(), page++, PAGE_SIZE);
                        while (results.size() > 0) {
                            resultsToRemove = this.removeDuplicateResultsBetweenCodeSets(results);
                            if (!(resultsToRemove.isEmpty())) {
                                results.removeAll(resultsToRemove);
                            }
                            saveToCSV(results, sectionName, fieldIndexes, extractId, true);
                            results = AllergyExtracts.runBulkAllergyAllCodesQuery(extractId, codeSet.getCodeSetId(), page++, PAGE_SIZE);
                        }
                        if (currentTransactionId > 0) {
                            results = AllergyExtracts.runDeltaAllergyAllCodesQuery(extractId, codeSet.getCodeSetId(),
                                    currentTransactionId, maxTransactionId);
                            resultsToRemove = this.removeDuplicateResultsBetweenCodeSets(results);
                            if (!(resultsToRemove.isEmpty())) {
                                results.removeAll(resultsToRemove);
                            }
                            saveToCSV(results, sectionName, fieldIndexes, extractId, true);
                        }
                        break;
                    case "earliest_each":
                        page = 1;
                        results = AllergyExtracts.runBulkAllergyEarliestEachCodesQuery(extractId, codeSet.getCodeSetId(), page++, PAGE_SIZE);
                        while (results.size() > 0) {
                            resultsToRemove = this.removeDuplicateResultsBetweenCodeSets(results);
                            if (!(resultsToRemove.isEmpty())) {
                                results.removeAll(resultsToRemove);
                            }
                            saveToCSV(results, sectionName, fieldIndexes, extractId, true);
                            results = AllergyExtracts.runBulkAllergyEarliestEachCodesQuery(extractId, codeSet.getCodeSetId(), page++, PAGE_SIZE);
                        }
                        if (currentTransactionId > 0) {
                            results = AllergyExtracts.runDeltaAllergyEarliestEachCodesQuery(extractId, codeSet.getCodeSetId(),
                                    currentTransactionId, maxTransactionId);
                            resultsToRemove = this.removeDuplicateResultsBetweenCodeSets(results);
                            if (!(resultsToRemove.isEmpty())) {
                                results.removeAll(resultsToRemove);
                            }
                            saveToCSV(results, sectionName, fieldIndexes, extractId, true);
                        }
                        break;
                    case "latest_each":
                        page = 1;
                        results = AllergyExtracts.runBulkAllergyLatestEachCodesQuery(extractId, codeSet.getCodeSetId(), page++, PAGE_SIZE);
                        while (results.size() > 0) {
                            resultsToRemove = this.removeDuplicateResultsBetweenCodeSets(results);
                            if (!(resultsToRemove.isEmpty())) {
                                results.removeAll(resultsToRemove);
                            }
                            saveToCSV(results, sectionName, fieldIndexes, extractId, true);
                            results = AllergyExtracts.runBulkAllergyLatestEachCodesQuery(extractId, codeSet.getCodeSetId(), page++, PAGE_SIZE);
                        }
                        if (currentTransactionId > 0) {
                            results = AllergyExtracts.runDeltaAllergyLatestEachCodesQuery(extractId, codeSet.getCodeSetId(),
                                    currentTransactionId, maxTransactionId);
                            resultsToRemove = this.removeDuplicateResultsBetweenCodeSets(results);
                            if (!(resultsToRemove.isEmpty())) {
                                results.removeAll(resultsToRemove);
                            }
                            saveToCSV(results, sectionName, fieldIndexes, extractId, true);
                        }
                        break;
                    case "earliest":
                        page = 1;
                        results = AllergyExtracts.runBulkAllergyEarliestCodesQuery(extractId, codeSet.getCodeSetId(), page++, PAGE_SIZE);
                        while (results.size() > 0) {
                            resultsToRemove = this.removeDuplicateResultsBetweenCodeSets(results);
                            if (!(resultsToRemove.isEmpty())) {
                                results.removeAll(resultsToRemove);
                            }
                            saveToCSV(results, sectionName, fieldIndexes, extractId, true);
                            results = AllergyExtracts.runBulkAllergyEarliestCodesQuery(extractId, codeSet.getCodeSetId(), page++, PAGE_SIZE);
                        }
                        if (currentTransactionId > 0) {
                            results = AllergyExtracts.runDeltaAllergyEarliestCodesQuery(extractId, codeSet.getCodeSetId(),
                                    currentTransactionId, maxTransactionId);
                            resultsToRemove = this.removeDuplicateResultsBetweenCodeSets(results);
                            if (!(resultsToRemove.isEmpty())) {
                                results.removeAll(resultsToRemove);
                            }
                            saveToCSV(results, sectionName, fieldIndexes, extractId, true);
                        }
                        break;
                    case "latest":
                        page = 1;
                        results = AllergyExtracts.runBulkAllergyLatestCodesQuery(extractId, codeSet.getCodeSetId(), page++, PAGE_SIZE);
                        while (results.size() > 0) {
                            resultsToRemove = this.removeDuplicateResultsBetweenCodeSets(results);
                            if (!(resultsToRemove.isEmpty())) {
                                results.removeAll(resultsToRemove);
                            }
                            saveToCSV(results, sectionName, fieldIndexes, extractId, true);
                            results = AllergyExtracts.runBulkAllergyLatestCodesQuery(extractId, codeSet.getCodeSetId(), page++, PAGE_SIZE);
                        }
                        if (currentTransactionId > 0) {
                            results = AllergyExtracts.runDeltaAllergyLatestCodesQuery(extractId, codeSet.getCodeSetId(),
                                    currentTransactionId, maxTransactionId);
                            resultsToRemove = this.removeDuplicateResultsBetweenCodeSets(results);
                            if (!(resultsToRemove.isEmpty())) {
                                results.removeAll(resultsToRemove);
                            }
                            saveToCSV(results, sectionName, fieldIndexes, extractId, true);
                        }
                        break;
                }
            }
            currentResultsIds.clear();
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
                List resultsToRemove;
                int page;
                switch (codeSet.getExtractType()) {
                    case "all":
                        page = 1;
                        results = ImmunisationExtracts.runBulkImmunisationAllCodesQuery(extractId, codeSet.getCodeSetId(), page++, PAGE_SIZE);
                        while (results.size() > 0) {
                            resultsToRemove = this.removeDuplicateResultsBetweenCodeSets(results);
                            if (!(resultsToRemove.isEmpty())) {
                                results.removeAll(resultsToRemove);
                            }
                            saveToCSV(results, sectionName, fieldIndexes, extractId, true);
                            results = ImmunisationExtracts.runBulkImmunisationAllCodesQuery(extractId, codeSet.getCodeSetId(), page++, PAGE_SIZE);
                        }
                        if (currentTransactionId > 0) {
                            results = ImmunisationExtracts.runDeltaImmunisationAllCodesQuery(extractId, codeSet.getCodeSetId(),
                                    currentTransactionId, maxTransactionId);
                            resultsToRemove = this.removeDuplicateResultsBetweenCodeSets(results);
                            if (!(resultsToRemove.isEmpty())) {
                                results.removeAll(resultsToRemove);
                            }
                            saveToCSV(results, sectionName, fieldIndexes, extractId, true);
                        }
                        break;
                    case "earliest_each":
                        page = 1;
                        results = ImmunisationExtracts.runBulkImmunisationEarliestEachCodesQuery(extractId, codeSet.getCodeSetId(), page++, PAGE_SIZE);
                        while (results.size() > 0) {
                            resultsToRemove = this.removeDuplicateResultsBetweenCodeSets(results);
                            if (!(resultsToRemove.isEmpty())) {
                                results.removeAll(resultsToRemove);
                            }
                            saveToCSV(results, sectionName, fieldIndexes, extractId, true);
                            results = ImmunisationExtracts.runBulkImmunisationEarliestEachCodesQuery(extractId, codeSet.getCodeSetId(), page++, PAGE_SIZE);
                        }
                        if (currentTransactionId > 0) {
                            results = ImmunisationExtracts.runDeltaImmunisationEarliestEachCodesQuery(extractId, codeSet.getCodeSetId(),
                                    currentTransactionId, maxTransactionId);
                            resultsToRemove = this.removeDuplicateResultsBetweenCodeSets(results);
                            if (!(resultsToRemove.isEmpty())) {
                                results.removeAll(resultsToRemove);
                            }
                            saveToCSV(results, sectionName, fieldIndexes, extractId, true);
                        }
                        break;
                    case "latest_each":
                        page = 1;
                        results = ImmunisationExtracts.runBulkImmunisationLatestEachCodesQuery(extractId, codeSet.getCodeSetId(), page++, PAGE_SIZE);
                        while (results.size() > 0) {
                            resultsToRemove = this.removeDuplicateResultsBetweenCodeSets(results);
                            if (!(resultsToRemove.isEmpty())) {
                                results.removeAll(resultsToRemove);
                            }
                            saveToCSV(results, sectionName, fieldIndexes, extractId, true);
                            results = ImmunisationExtracts.runBulkImmunisationLatestEachCodesQuery(extractId, codeSet.getCodeSetId(), page++, PAGE_SIZE);
                        }
                        if (currentTransactionId > 0) {
                            results = ImmunisationExtracts.runDeltaImmunisationLatestEachCodesQuery(extractId, codeSet.getCodeSetId(),
                                    currentTransactionId, maxTransactionId);
                            resultsToRemove = this.removeDuplicateResultsBetweenCodeSets(results);
                            if (!(resultsToRemove.isEmpty())) {
                                results.removeAll(resultsToRemove);
                            }
                            saveToCSV(results, sectionName, fieldIndexes, extractId, true);
                        }
                        break;
                    case "earliest":
                        page = 1;
                        results = ImmunisationExtracts.runBulkImmunisationEarliestCodesQuery(extractId, codeSet.getCodeSetId(), page++, PAGE_SIZE);
                        while (results.size() > 0) {
                            resultsToRemove = this.removeDuplicateResultsBetweenCodeSets(results);
                            if (!(resultsToRemove.isEmpty())) {
                                results.removeAll(resultsToRemove);
                            }
                            saveToCSV(results, sectionName, fieldIndexes, extractId, true);
                            results = ImmunisationExtracts.runBulkImmunisationEarliestCodesQuery(extractId, codeSet.getCodeSetId(), page++, PAGE_SIZE);
                        }
                        if (currentTransactionId > 0) {
                            results = ImmunisationExtracts.runDeltaImmunisationEarliestCodesQuery(extractId, codeSet.getCodeSetId(),
                                    currentTransactionId, maxTransactionId);
                            resultsToRemove = this.removeDuplicateResultsBetweenCodeSets(results);
                            if (!(resultsToRemove.isEmpty())) {
                                results.removeAll(resultsToRemove);
                            }
                            saveToCSV(results, sectionName, fieldIndexes, extractId, true);
                        }
                        break;
                    case "latest":
                        page = 1;
                        results = ImmunisationExtracts.runBulkImmunisationLatestCodesQuery(extractId, codeSet.getCodeSetId(), page++, PAGE_SIZE);
                        while (results.size() > 0) {
                            resultsToRemove = this.removeDuplicateResultsBetweenCodeSets(results);
                            if (!(resultsToRemove.isEmpty())) {
                                results.removeAll(resultsToRemove);
                            }
                            saveToCSV(results, sectionName, fieldIndexes, extractId, true);
                            results = ImmunisationExtracts.runBulkImmunisationLatestCodesQuery(extractId, codeSet.getCodeSetId(), page++, PAGE_SIZE);
                        }
                        if (currentTransactionId > 0) {
                            results = ImmunisationExtracts.runDeltaImmunisationLatestCodesQuery(extractId, codeSet.getCodeSetId(),
                                    currentTransactionId, maxTransactionId);
                            resultsToRemove = this.removeDuplicateResultsBetweenCodeSets(results);
                            if (!(resultsToRemove.isEmpty())) {
                                results.removeAll(resultsToRemove);
                            }
                            saveToCSV(results, sectionName, fieldIndexes, extractId, true);
                        }
                        break;
                }
            }
            currentResultsIds.clear();
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
                List resultsToRemove;
                int page;
                switch (codeSet.getExtractType()) {
                    case "all":
                        page = 1;
                        results = MedicationExtracts.runBulkMedicationAllCodesQuery(extractId, codeSet.getCodeSetId(), page++, PAGE_SIZE);
                        while (results.size() > 0) {
                            resultsToRemove = this.removeDuplicateResultsBetweenCodeSets(results);
                            if (!(resultsToRemove.isEmpty())) {
                                results.removeAll(resultsToRemove);
                            }
                            saveToCSV(results, sectionName, fieldIndexes, extractId, true);
                            results = MedicationExtracts.runBulkMedicationAllCodesQuery(extractId, codeSet.getCodeSetId(), page++, PAGE_SIZE);
                        }
                        if (currentTransactionId > 0) {
                            results = MedicationExtracts.runDeltaMedicationAllCodesQuery(extractId, codeSet.getCodeSetId(),
                                    currentTransactionId, maxTransactionId);
                            resultsToRemove = this.removeDuplicateResultsBetweenCodeSets(results);
                            if (!(resultsToRemove.isEmpty())) {
                                results.removeAll(resultsToRemove);
                            }
                            saveToCSV(results, sectionName, fieldIndexes, extractId, true);
                        }
                        break;
                    case "earliest_each":
                        page = 1;
                        results = MedicationExtracts.runBulkMedicationEarliestEachCodesQuery(extractId, codeSet.getCodeSetId(), page++, PAGE_SIZE);
                        while (results.size() > 0) {
                            resultsToRemove = this.removeDuplicateResultsBetweenCodeSets(results);
                            if (!(resultsToRemove.isEmpty())) {
                                results.removeAll(resultsToRemove);
                            }
                            saveToCSV(results, sectionName, fieldIndexes, extractId, true);
                            results = MedicationExtracts.runBulkMedicationEarliestEachCodesQuery(extractId, codeSet.getCodeSetId(), page++, PAGE_SIZE);
                        }
                        if (currentTransactionId > 0) {
                            results = MedicationExtracts.runDeltaMedicationEarliestEachCodesQuery(extractId, codeSet.getCodeSetId(),
                                    currentTransactionId, maxTransactionId);
                            resultsToRemove = this.removeDuplicateResultsBetweenCodeSets(results);
                            if (!(resultsToRemove.isEmpty())) {
                                results.removeAll(resultsToRemove);
                            }
                            saveToCSV(results, sectionName, fieldIndexes, extractId, true);
                        }
                        break;
                    case "latest_each":
                        page = 1;
                        results = MedicationExtracts.runBulkMedicationLatestEachCodesQuery(extractId, codeSet.getCodeSetId(), page++, PAGE_SIZE);
                        while (results.size() > 0) {
                            resultsToRemove = this.removeDuplicateResultsBetweenCodeSets(results);
                            if (!(resultsToRemove.isEmpty())) {
                                results.removeAll(resultsToRemove);
                            }
                            saveToCSV(results, sectionName, fieldIndexes, extractId, true);
                            results = MedicationExtracts.runBulkMedicationLatestEachCodesQuery(extractId, codeSet.getCodeSetId(), page++, PAGE_SIZE);
                        }
                        if (currentTransactionId > 0) {
                            results = MedicationExtracts.runDeltaMedicationLatestEachCodesQuery(extractId, codeSet.getCodeSetId(),
                                    currentTransactionId, maxTransactionId);
                            resultsToRemove = this.removeDuplicateResultsBetweenCodeSets(results);
                            if (!(resultsToRemove.isEmpty())) {
                                results.removeAll(resultsToRemove);
                            }
                            saveToCSV(results, sectionName, fieldIndexes, extractId, true);
                        }
                        break;
                    case "earliest":
                        page = 1;
                        results = MedicationExtracts.runBulkMedicationEarliestCodesQuery(extractId, codeSet.getCodeSetId(), page++, PAGE_SIZE);
                        while (results.size() > 0) {
                            resultsToRemove = this.removeDuplicateResultsBetweenCodeSets(results);
                            if (!(resultsToRemove.isEmpty())) {
                                results.removeAll(resultsToRemove);
                            }
                            saveToCSV(results, sectionName, fieldIndexes, extractId, true);
                            results = MedicationExtracts.runBulkMedicationEarliestCodesQuery(extractId, codeSet.getCodeSetId(), page++, PAGE_SIZE);
                        }
                        if (currentTransactionId > 0) {
                            results = MedicationExtracts.runDeltaMedicationEarliestCodesQuery(extractId, codeSet.getCodeSetId(),
                                    currentTransactionId, maxTransactionId);
                            resultsToRemove = this.removeDuplicateResultsBetweenCodeSets(results);
                            if (!(resultsToRemove.isEmpty())) {
                                results.removeAll(resultsToRemove);
                            }
                            saveToCSV(results, sectionName, fieldIndexes, extractId, true);
                        }
                        break;
                    case "latest":
                        page = 1;
                        results = MedicationExtracts.runBulkMedicationLatestCodesQuery(extractId, codeSet.getCodeSetId(), page++, PAGE_SIZE);
                        while (results.size() > 0) {
                            resultsToRemove = this.removeDuplicateResultsBetweenCodeSets(results);
                            if (!(resultsToRemove.isEmpty())) {
                                results.removeAll(resultsToRemove);
                            }
                            saveToCSV(results, sectionName, fieldIndexes, extractId, true);
                            results = MedicationExtracts.runBulkMedicationLatestCodesQuery(extractId, codeSet.getCodeSetId(), page++, PAGE_SIZE);
                        }
                        if (currentTransactionId > 0) {
                            results = MedicationExtracts.runDeltaMedicationLatestCodesQuery(extractId, codeSet.getCodeSetId(),
                                    currentTransactionId, maxTransactionId);
                            resultsToRemove = this.removeDuplicateResultsBetweenCodeSets(results);
                            if (!(resultsToRemove.isEmpty())) {
                                results.removeAll(resultsToRemove);
                            }
                            saveToCSV(results, sectionName, fieldIndexes, extractId, true);
                        }
                        break;
                }
            }
            currentResultsIds.clear();
        }
    }

    private void runFinaliseExtract(int extractId, Long maxTransactionId) throws Exception {

        GeneralQueries.setBulkedStatus(extractId);
        GeneralQueries.setTransactionId(extractId, maxTransactionId);
    }

    private void createCSV(List<String> fieldHeaders, String tableName, int extractId) throws Exception {

        String sourceLocation = this.createSourceDirectoryString(extractId);
        String extractIdAndTodayDate = this.createExtractIdAndTodayDateString(extractId);
        String filename = this.createFilename(sourceLocation, extractIdAndTodayDate, tableName);

        FileWriter fw = new FileWriter(filename);
        try {
            int counter = 0;
            for (String field : fieldHeaders) {
                fw.append("\"" + field + "\"");
                counter++;
                if (counter < fieldHeaders.size()) {
                    fw.append(',');
                }
            }
            fw.append(System.getProperty("line.separator"));
        } finally {
            fw.flush();
            fw.close();
            // System.out.println("Headers saved in " + tableName + ".csv");
            LOG.info(filename + " file created, with headers only");
        }
    }

    private void saveToCSV(List<Object[]> results, String tableName, List<Integer> fieldIndexes, int extractId,
                           boolean saveIds) throws Exception {

        String sourceLocation = this.createSourceDirectoryString(extractId);
        String extractIdAndTodayDate = this.createExtractIdAndTodayDateString(extractId);
        String filename = this.createFilename(sourceLocation, extractIdAndTodayDate, tableName);

        List<Long> itemIds = new ArrayList<>();

        // FileWriter fw = new FileWriter(filename, true);
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(filename, true))) {
            // LOG.info(results.size() + " records returned" );
            for (Object[] result : results) {
                int counter = 0;
                for (Integer idx : fieldIndexes) {

                    // idx of 0 is always the item id so add to the item Id list for saving later
                    if (saveIds && idx == 0) {
                        itemIds.add(Long.parseLong(result[idx].toString()));
                    }

                    if (result[idx] != null) {
                        bw.append("\"" + result[idx].toString() + "\"");
                    } else {
                        bw.append("\"\"");
                    }
                    counter++;
                    if (counter < fieldIndexes.size()) {
                        bw.append(',');
                    }
                }
                bw.append(System.getProperty("line.separator"));
            }
        } finally {
            /*fw.flush();
            fw.close();*/
            // System.out.println("data added to " + tableName);
            // LOG.info("All rows of data added to " + filename);
        }

        // save the exported Ids for tracking later
        if (saveIds) {
            ExportedIdsEntity.saveExportedIds(extractId, getTableId(tableName), itemIds);
        }
    }

    private void createSourceAndHousekeepDirectories(int extractId) throws Exception {

        // creates directory named by sourceLocation pathname, and any necessary non-existent
        // parent directories, so useful for first run of any new extract added to the database
        String sourceLocation = this.createSourceDirectoryString(extractId);
        File sourceLocDir = new File(sourceLocation);
        if (!(sourceLocDir.exists())) {
            sourceLocDir.mkdirs();
        }

        // creates directory named by housekeepLocation pathname, only creating that directory,
        // as the rest of the folder structure, within which it sits, has been created above
        ExtractConfig config = ExtractCache.getExtractConfig(extractId);
        String housekeepLocation = config.getFileLocationDetails().getHousekeep();
        if (!(housekeepLocation.endsWith(File.separator))) {
            housekeepLocation += File.separator;
        }
        File houseLocDir = new File(housekeepLocation);
        if (!(houseLocDir.exists())) {
            houseLocDir.mkdir();
        }
    }

    private String createSourceDirectoryString(int extractId) throws Exception {

        ExtractConfig config = ExtractCache.getExtractConfig(extractId);
        String sourceLocation = config.getFileLocationDetails().getSource();
        if (!(sourceLocation.endsWith(File.separator))) {
            sourceLocation += File.separator;
        }
        return sourceLocation;
    }

    private void createTodayDirectory(String sourceLocation, String extractIdAndTodayDate) {

        String strTodayDir = sourceLocation + extractIdAndTodayDate;
        if (!(strTodayDir.endsWith(File.separator))) {
            strTodayDir += File.separator;
        }
        File todayDir = new File(strTodayDir);
        if (!(todayDir.exists())) {
            todayDir.mkdir();
        }
    }

    private String createExtractIdAndTodayDateString(int extractId) {

        Date todayDate = Calendar.getInstance().getTime();
        DateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
        String strTodayDate = dateFormat.format(todayDate);
        String extractIdAndTodayDate = extractId + "_" + strTodayDate;
        return extractIdAndTodayDate;
    }

    private String createFilename(String sourceLocation, String extractIdAndTodayDate, String tableName) {

        String strTodayDir = sourceLocation + extractIdAndTodayDate;
        if (!(strTodayDir.endsWith(File.separator))) {
            strTodayDir += File.separator;
        }
        String filename = strTodayDir + extractIdAndTodayDate + "_" + tableName + ".csv";
        return filename;
    }

    private int getTableId(String tableName) throws Exception {
        switch (tableName) {
            case "observation":
                return 32;
            case "patient":
                return 8;
            case "immunisation":
                return 40;
            case "medication":
                return 44;
            case "allergy":
                return 41;
            default:
                throw new Exception("Table Id not found : " + tableName);
        }
    }

    private List<Object[]> removeDuplicateResultsBetweenCodeSets(List<Object[]> results) {

        List<Object[]> resultsToRemove = new ArrayList<>();
        for (Object[] result : results) {
            Long resultId = Long.parseLong(result[0].toString());
            if (currentResultsIds.contains(resultId)) {
                resultsToRemove.add(result);
            } else {
                currentResultsIds.add(resultId);
            }
        }
        return resultsToRemove;
    }
}