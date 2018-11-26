package org.endeavourhealth.scheduler.job;

import org.endeavourhealth.scheduler.cache.DatasetCache;
import org.endeavourhealth.scheduler.cache.ExtractCache;
import org.endeavourhealth.scheduler.json.DatasetDefinition.DatasetCodeSet;
import org.endeavourhealth.scheduler.json.DatasetDefinition.DatasetConfig;
import org.endeavourhealth.scheduler.json.DatasetDefinition.DatasetConfigExtract;
import org.endeavourhealth.scheduler.json.DatasetDefinition.DatasetFields;
import org.endeavourhealth.scheduler.models.CustomExtracts.AllergyExtracts;
import org.endeavourhealth.scheduler.models.CustomExtracts.ImmunisationExtracts;
import org.endeavourhealth.scheduler.models.PersistenceManager;
import org.endeavourhealth.scheduler.models.database.ExtractEntity;
import org.endeavourhealth.scheduler.models.CustomExtracts.ObservationExtracts;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.persistence.EntityManager;
import javax.persistence.ParameterMode;
import javax.persistence.Query;
import javax.persistence.StoredProcedureQuery;
import java.io.FileWriter;
import java.math.BigInteger;
import java.util.List;
import java.util.stream.Collectors;

public class GenerateData implements Job {

    private static final Logger LOG = LoggerFactory.getLogger(GenerateData.class);
    private boolean limitCols = false;

    public void setLimitCols(boolean limit) {
        this.limitCols = limit;
    }

    public void execute(JobExecutionContext jobExecutionContext) {

        System.out.println("Generate Data");

        //TODO Run the data extractor Stored Proc to move the data into new temporary tables

        try {

            // TODO figure out which extracts are ready to run (after cohort has been defined)
            int extractId = 1;
            processExtracts(extractId);
        } catch (Exception e) {
            System.out.println("Error: " + e.getMessage());
        }

        System.out.println("End of Generate Data");
    }

    private void processExtracts(int extractId) throws Exception {

        ExtractEntity extractDetails = ExtractCache.getExtract(extractId);

        DatasetConfig datasetConfig = DatasetCache.getDatasetConfig(extractDetails.getDatasetId());

        if (datasetConfig.getExtract() != null) {

            Long maxTransactionId = getMaxTransactionId();

            for (DatasetConfigExtract extract : datasetConfig.getExtract()) {
                System.out.println(extract.getType());
                switch (extract.getType()) {
                    case "patient":
                        // runObservationExtractForCodeSets(extract.getCodeSets(), extractId, "observation", maxTransactionId);
                        break;
                    case "medication":
                        /*if (limitCols) {
                            runGenericStatusExtract(extract, extractId, "generate_medication");
                        } else {
                            runGenericStatusExtractAll(extract, extractId, "generate_medication_all_col",
                                    extractDetails.getCodeSetId(), maxTransactionId);
                        }*/
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

    private void runObservationExtractForCodeSets(DatasetConfigExtract extractConfig, int extractId, String sectionName, Long currentTransactionId, Long maxTransactionId) throws Exception {

        List<DatasetCodeSet> codeSets = extractConfig.getCodeSets();

        List<String> fieldHeaders = extractConfig.getFields().stream().map(DatasetFields::getHeader).collect(Collectors.toList());
        List<Integer> fieldIndexes = extractConfig.getFields().stream().map(DatasetFields::getIndex).collect(Collectors.toList());

        if (codeSets != null && codeSets.size() > 0) {

            // create the headers and the actual file
            createCSV(fieldHeaders, sectionName);

            for (DatasetCodeSet codeSet : codeSets) {
                List results;
                switch (codeSet.getExtractType())
                {
                    case "all":
                        results = ObservationExtracts.runBulkObservationAllCodesQuery(extractId, codeSet.getCodeSetId());
                        saveToCSV(results, sectionName, fieldIndexes);
                        results = ObservationExtracts.runDeltaObservationAllCodesQuery(extractId, codeSet.getCodeSetId(),
                                currentTransactionId, maxTransactionId);
                        saveToCSV(results, sectionName, fieldIndexes);
                        break;
                    case "earliest_each":
                        results = ObservationExtracts.runBulkObservationEarliestEachCodesQuery(extractId, codeSet.getCodeSetId());
                        saveToCSV(results, sectionName, fieldIndexes);
                        results = ObservationExtracts.runDeltaObservationEarliestEachCodesQuery(extractId, codeSet.getCodeSetId(),
                                currentTransactionId, maxTransactionId);
                        saveToCSV(results, sectionName, fieldIndexes);
                        break;
                    case "latest_each":
                        results = ObservationExtracts.runBulkObservationLatestEachCodesQuery(extractId, codeSet.getCodeSetId());
                        saveToCSV(results, sectionName, fieldIndexes);
                        results = ObservationExtracts.runDeltaObservationLatestEachCodesQuery(extractId, codeSet.getCodeSetId(),
                                currentTransactionId, maxTransactionId);
                        saveToCSV(results, sectionName, fieldIndexes);
                        break;
                    case "earliest":
                        results = ObservationExtracts.runBulkObservationEarliestCodesQuery(extractId, codeSet.getCodeSetId());
                        saveToCSV(results, sectionName, fieldIndexes);
                        results = ObservationExtracts.runDeltaObservationEarliestCodesQuery(extractId, codeSet.getCodeSetId(),
                                currentTransactionId, maxTransactionId);
                        saveToCSV(results, sectionName, fieldIndexes);
                        break;
                    case "latest":
                        results = ObservationExtracts.runBulkObservationLatestCodesQuery(extractId, codeSet.getCodeSetId());
                        saveToCSV(results, sectionName, fieldIndexes);
                        results = ObservationExtracts.runDeltaObservationLatestCodesQuery(extractId, codeSet.getCodeSetId(),
                                currentTransactionId, maxTransactionId);
                        saveToCSV(results, sectionName, fieldIndexes);
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
            createCSV(fieldHeaders, sectionName);

            for (DatasetCodeSet codeSet : codeSets) {
                List results;
                switch (codeSet.getExtractType())
                {
                    case "all":
                        results = AllergyExtracts.runBulkAllergyAllCodesQuery(extractId, codeSet.getCodeSetId());
                        saveToCSV(results, sectionName, fieldIndexes);
                        results = AllergyExtracts.runDeltaAllergyAllCodesQuery(extractId, codeSet.getCodeSetId(),
                                currentTransactionId, maxTransactionId);
                        saveToCSV(results, sectionName, fieldIndexes);
                        break;
                    case "earliest_each":
                        results = AllergyExtracts.runBulkAllergyEarliestEachCodesQuery(extractId, codeSet.getCodeSetId());
                        saveToCSV(results, sectionName, fieldIndexes);
                        results = AllergyExtracts.runDeltaAllergyEarliestEachCodesQuery(extractId, codeSet.getCodeSetId(),
                                currentTransactionId, maxTransactionId);
                        saveToCSV(results, sectionName, fieldIndexes);
                        break;
                    case "latest_each":
                        results = AllergyExtracts.runBulkAllergyLatestEachCodesQuery(extractId, codeSet.getCodeSetId());
                        saveToCSV(results, sectionName, fieldIndexes);
                        results = AllergyExtracts.runDeltaAllergyLatestEachCodesQuery(extractId, codeSet.getCodeSetId(),
                                currentTransactionId, maxTransactionId);
                        saveToCSV(results, sectionName, fieldIndexes);
                        break;
                    case "earliest":
                        results = AllergyExtracts.runBulkAllergyEarliestCodesQuery(extractId, codeSet.getCodeSetId());
                        saveToCSV(results, sectionName, fieldIndexes);
                        results = AllergyExtracts.runDeltaAllergyEarliestCodesQuery(extractId, codeSet.getCodeSetId(),
                                currentTransactionId, maxTransactionId);
                        saveToCSV(results, sectionName, fieldIndexes);
                        break;
                    case "latest":
                        results = AllergyExtracts.runBulkAllergyLatestCodesQuery(extractId, codeSet.getCodeSetId());
                        saveToCSV(results, sectionName, fieldIndexes);
                        results = AllergyExtracts.runDeltaAllergyLatestCodesQuery(extractId, codeSet.getCodeSetId(),
                                currentTransactionId, maxTransactionId);
                        saveToCSV(results, sectionName, fieldIndexes);
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
            createCSV(fieldHeaders, sectionName);

            for (DatasetCodeSet codeSet : codeSets) {
                List results;
                switch (codeSet.getExtractType())
                {
                    case "all":
                        results = ImmunisationExtracts.runBulkImmunisationAllCodesQuery(extractId, codeSet.getCodeSetId());
                        saveToCSV(results, sectionName, fieldIndexes);
                        results = ImmunisationExtracts.runDeltaImmunisationAllCodesQuery(extractId, codeSet.getCodeSetId(),
                                currentTransactionId, maxTransactionId);
                        saveToCSV(results, sectionName, fieldIndexes);
                        break;
                    case "earliest_each":
                        results = ImmunisationExtracts.runBulkImmunisationEarliestEachCodesQuery(extractId, codeSet.getCodeSetId());
                        saveToCSV(results, sectionName, fieldIndexes);
                        results = ImmunisationExtracts.runDeltaImmunisationEarliestEachCodesQuery(extractId, codeSet.getCodeSetId(),
                                currentTransactionId, maxTransactionId);
                        saveToCSV(results, sectionName, fieldIndexes);
                        break;
                    case "latest_each":
                        results = ImmunisationExtracts.runBulkImmunisationLatestEachCodesQuery(extractId, codeSet.getCodeSetId());
                        saveToCSV(results, sectionName, fieldIndexes);
                        results = ImmunisationExtracts.runDeltaImmunisationLatestEachCodesQuery(extractId, codeSet.getCodeSetId(),
                                currentTransactionId, maxTransactionId);
                        saveToCSV(results, sectionName, fieldIndexes);
                        break;
                    case "earliest":
                        results = ImmunisationExtracts.runBulkImmunisationEarliestCodesQuery(extractId, codeSet.getCodeSetId());
                        saveToCSV(results, sectionName, fieldIndexes);
                        results = ImmunisationExtracts.runDeltaImmunisationEarliestCodesQuery(extractId, codeSet.getCodeSetId(),
                                currentTransactionId, maxTransactionId);
                        saveToCSV(results, sectionName, fieldIndexes);
                        break;
                    case "latest":
                        results = ImmunisationExtracts.runBulkImmunisationLatestCodesQuery(extractId, codeSet.getCodeSetId());
                        saveToCSV(results, sectionName, fieldIndexes);
                        results = ImmunisationExtracts.runDeltaImmunisationLatestCodesQuery(extractId, codeSet.getCodeSetId(),
                                currentTransactionId, maxTransactionId);
                        saveToCSV(results, sectionName, fieldIndexes);
                        break;
                }
            }
        }
    }

    private Long getMaxTransactionId() throws Exception {
        EntityManager entityManager = PersistenceManager.getEntityManager();

        try {
            String sql = "select max(id) from pcr.event_log;";
            Query query = entityManager.createNativeQuery(sql);

            BigInteger result = (BigInteger)query.getSingleResult();

            return result.longValue();

        } finally {
            entityManager.close();
        }
    }

    private void runFinaliseExtract(int extractId, Long maxTransactionId) throws Exception {
        EntityManager entityManager = PersistenceManager.getEntityManager();

        try {

            System.out.println("Running finalise_extract");


            StoredProcedureQuery query = entityManager.createStoredProcedureQuery("finalise_extract")
                    .registerStoredProcedureParameter("extractId", Integer.class, ParameterMode.IN)
                    .registerStoredProcedureParameter("maxTransactionId", Long.class, ParameterMode.IN)
                    .setParameter("extractId", extractId)
                    .setParameter("maxTransactionId", maxTransactionId);

            query.execute();
            System.out.println("finalise_extract executed");
        } finally {
            entityManager.close();
        }
    }

    private void createCSV(List<String> fieldHeaders, String tableName) throws Exception {
        String filename = "C:\\sftpkey\\" + tableName + ".csv";

        FileWriter fw = new FileWriter(filename);
        try {
            for (String field: fieldHeaders) {
                fw.append(field);
                fw.append(',');
            }
            fw.append(System.getProperty("line.separator"));
        } finally {
            fw.flush();
            fw.close();
            System.out.println("Headers saved in " + tableName + ".csv");
        }
    }

    private void saveToCSV(List<Object[]> results,  String tableName, List<Integer> fieldIndexes) throws Exception {
        String filename = "C:\\sftpkey\\" + tableName + ".csv";

        FileWriter fw = new FileWriter(filename, true);
        try {
            for (Object[] result : results) {

                for (Integer idx : fieldIndexes) {
                    if (result[idx] != null) {
                        fw.append(result[idx].toString());
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
