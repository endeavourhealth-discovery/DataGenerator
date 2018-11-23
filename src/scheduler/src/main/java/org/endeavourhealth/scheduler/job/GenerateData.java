package org.endeavourhealth.scheduler.job;

import org.endeavourhealth.scheduler.cache.DatasetCache;
import org.endeavourhealth.scheduler.cache.ExtractCache;
import org.endeavourhealth.scheduler.json.DatasetCodeSet;
import org.endeavourhealth.scheduler.json.DatasetConfig;
import org.endeavourhealth.scheduler.json.DatasetConfigExtract;
import org.endeavourhealth.scheduler.models.PersistenceManager;
import org.endeavourhealth.scheduler.models.database.ExtractEntity;
import org.endeavourhealth.scheduler.models.database.ExtractSQL;
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
import java.util.ArrayList;
import java.util.List;

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
                        // runExtractForCodeSets(extract.getCodeSets(), extractId, "observation", maxTransactionId);
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
                        runExtractForCodeSets(extract, extractId, "observation", maxTransactionId);
                        break;
                    case "allergy":
                        /*if (limitCols) {
                            runGenericStatusExtract(extract, extractId, "generate_allergy");
                        } else {
                            runGenericStatusExtractAll(extract, extractId, "generate_allergy_all_col",
                                    extractDetails.getCodeSetId(), maxTransactionId);
                        }*/
                        break;
                    case "immunisation":
                        /*if (limitCols) {
                            runGenericExtract(extract, extractId, "generate_immunisation");
                        } else {
                            runGenericExtractAll(extract, extractId, "generate_immunisation_all_col",
                                    extractDetails.getCodeSetId(), maxTransactionId);
                        }*/
                        break;
                }
            }

            runFinaliseExtract(extractId, maxTransactionId);
        }

    }

    private void runExtractForCodeSets(DatasetConfigExtract extractConfig, int extractId, String sectionName, Long maxTransactionId) throws Exception {

        List<DatasetCodeSet> codeSets = extractConfig.getCodeSets();

        if (codeSets != null && codeSets.size() > 0) {

            // create the headers and the actual file
            createCSV(extractConfig.getFields(), sectionName);

            for (DatasetCodeSet codeSet : codeSets) {
                String bulkProcName = "bulk_" + sectionName + "_" + codeSet.getExtractType() + "_in_code_set";
                String deltaProcName = "delta_" + sectionName + "_" + codeSet.getExtractType() + "_in_code_set";
                System.out.println(bulkProcName);

                List<Object[]> bulkResults = ExtractSQL.runBulkObservationAllCodesQuery(extractId, codeSet.getCodeSetId());
                // List<Object[]> bulkResults = executeBulkExtract(extractId, codeSet.getCodeSetId(), bulkProcName);
                saveToCSV(bulkResults, sectionName);
                // List<Object[]> deltaResults = executeDeltaExtract(extractId, codeSet.getCodeSetId(), deltaProcName, maxTransactionId);
                // saveToCSV(deltaResults, sectionName);
            }
        }
    }

    private List<Object[]> executeBulkExtract(int extractId, int codeSetId, String procedureName) throws Exception {

        long startTime = System.currentTimeMillis();
        EntityManager entityManager = PersistenceManager.getEntityManager();

        try {
            System.out.println("Running " + procedureName);

            StoredProcedureQuery query = entityManager.createStoredProcedureQuery(procedureName)
                    .registerStoredProcedureParameter("extractId", Integer.class, ParameterMode.IN)
                    .registerStoredProcedureParameter("codeSetId", Integer.class, ParameterMode.IN)
                    .setParameter("extractId", extractId)
                    .setParameter("codeSetId", extractId);

            List<Object[]> results = query.getResultList();
            long estimatedTime = System.currentTimeMillis() - startTime;
            System.out.println(procedureName + " executed, Time taken " + estimatedTime);

            return results;
        } finally {
            entityManager.close();
        }
    }

    private List<Object[]> executeDeltaExtract(int extractId, int codeSetId, String procedureName, Long maxTransactionId) throws Exception {
        long startTime = System.currentTimeMillis();
        EntityManager entityManager = PersistenceManager.getEntityManager();

        try {

            System.out.println("Running " + procedureName);

            StoredProcedureQuery query = entityManager.createStoredProcedureQuery(procedureName)
                    .registerStoredProcedureParameter("extractId", Integer.class, ParameterMode.IN)
                    .registerStoredProcedureParameter("codeSetId", Integer.class, ParameterMode.IN)
                    .registerStoredProcedureParameter("maxTransactionId", Long.class, ParameterMode.IN)
                    .setParameter("extractId", extractId)
                    .setParameter("codeSetId", codeSetId)
                    .setParameter("maxTransactionId", maxTransactionId);

            List<Object[]> results = query.getResultList();
            boolean hadResults = query.execute();
            if (hadResults) {
                List rs = query.getResultList();
                for (Object r : rs) {
                    System.out.println(r);
                }
            }

            long estimatedTime = System.currentTimeMillis() - startTime;
            System.out.println(procedureName + " executed, Time taken " + estimatedTime);

            return results;
        } finally {
            entityManager.close();
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

    private void createCSV(String fields, String tableName) throws Exception {
        String filename = "C:\\sftpkey\\" + tableName + ".csv";


        FileWriter fw = new FileWriter(filename);
        try {
            for (String field: fields.split(",")) {
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

    private void saveToCSV(List<Object[]> results,  String tableName) throws Exception {
        String filename = "C:\\sftpkey\\" + tableName + ".csv";

        List<Integer> fields = new ArrayList<>();
        fields.add(1);
        fields.add(3);
        fields.add(11);
        fields.add(12);
        FileWriter fw = new FileWriter(filename, true);
        try {
            for (Object[] result : results) {

                for (Integer idx : fields) {
                    if (result[idx] != null) {
                        fw.append(result[idx].toString());
                        fw.append(',');
                    }
                }
/*
                for (Object obj : result) {
                    if (obj != null) {
                        fw.append(obj.toString());
                        fw.append(',');
                    }
                }*/
                fw.append(System.getProperty("line.separator"));
            }
        } finally {
            fw.flush();
            fw.close();
            System.out.println("data added to " + tableName);
        }
    }
}
