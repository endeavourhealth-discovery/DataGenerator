package org.endeavourhealth.scheduler.job;

import org.endeavourhealth.scheduler.cache.DatasetCache;
import org.endeavourhealth.scheduler.cache.ExtractCache;
import org.endeavourhealth.scheduler.json.DatasetConfig;
import org.endeavourhealth.scheduler.json.DatasetConfigExtract;
import org.endeavourhealth.scheduler.models.PersistenceManager;
import org.endeavourhealth.scheduler.models.database.ExtractEntity;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.persistence.EntityManager;
import javax.persistence.ParameterMode;
import javax.persistence.Query;
import javax.persistence.StoredProcedureQuery;

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

            int maxTransactionId = getMaxTransactionId();

            for (DatasetConfigExtract extract : datasetConfig.getExtract()) {
                System.out.println(extract.getType());
                switch (extract.getType()) {
                    case "patient":
                        if (limitCols) {
                            runGenericExtract(extract, extractId, "generate_patient");
                        } else {
                            runGenericExtractAll(extract, extractId, "generate_patient_all_col",
                                    extractDetails.getCodeSetId(), maxTransactionId);
                        }
                        break;
                    case "medication":
                        if (limitCols) {
                            runGenericStatusExtract(extract, extractId, "generate_medication");
                        } else {
                            runGenericStatusExtractAll(extract, extractId, "generate_medication_all_col",
                                    extractDetails.getCodeSetId(), maxTransactionId);
                        }
                        break;
                    case "observation":
                        if (limitCols) {
                            runGenericExtract(extract, extractId, "generate_observation");
                        } else {
                            runGenericExtractAll(extract, extractId, "generate_observation_all_col",
                                    extractDetails.getCodeSetId(), maxTransactionId);
                        }
                        break;
                    case "allergy":
                        if (limitCols) {
                            runGenericStatusExtract(extract, extractId, "generate_allergy");
                        } else {
                            runGenericStatusExtractAll(extract, extractId, "generate_allergy_all_col",
                                    extractDetails.getCodeSetId(), maxTransactionId);
                        }
                        break;
                    case "immunisation":
                        if (limitCols) {
                            runGenericExtract(extract, extractId, "generate_immunisation");
                        } else {
                            runGenericExtractAll(extract, extractId, "generate_immunisation_all_col",
                                    extractDetails.getCodeSetId(), maxTransactionId);
                        }
                        break;
                }
            }

            runFinaliseExtract(extractId, maxTransactionId);
        }

    }

    private void runGenericExtract(DatasetConfigExtract extract, int extractId, String procedureName) throws Exception {

        long startTime = System.currentTimeMillis();
        EntityManager entityManager = PersistenceManager.getEntityManager();

        System.out.println("Running " + procedureName);

        StoredProcedureQuery query = entityManager.createStoredProcedureQuery(procedureName)
                .registerStoredProcedureParameter("col_list", String.class, ParameterMode.IN)
                .registerStoredProcedureParameter("extractId", Integer.class, ParameterMode.IN)
                .setParameter("col_list", extract.getFields())
                .setParameter("extractId", extractId);

        query.execute();
        long estimatedTime = System.currentTimeMillis() - startTime;
        System.out.println(procedureName + " executed, Time taken " + estimatedTime);
    }

    private void runGenericExtractAll(DatasetConfigExtract extract, int extractId, String procedureName,
                                      int codeSetId, int maxTransactionId) throws Exception {
        long startTime = System.currentTimeMillis();
        EntityManager entityManager = PersistenceManager.getEntityManager();

        System.out.println("Running " + procedureName);

        StoredProcedureQuery query = entityManager.createStoredProcedureQuery(procedureName)
                .registerStoredProcedureParameter("extractId", Integer.class, ParameterMode.IN)
                .registerStoredProcedureParameter("codeSetId", Integer.class, ParameterMode.IN)
                .registerStoredProcedureParameter("maxTransactionId", Integer.class, ParameterMode.IN)
                .setParameter("extractId", extractId)
                .setParameter("codeSetId", codeSetId)
                .setParameter("maxTransactionId", maxTransactionId);

        query.execute();
        long estimatedTime = System.currentTimeMillis() - startTime;
        System.out.println(procedureName + " executed, Time taken " + estimatedTime);
    }

    private void runGenericStatusExtract(DatasetConfigExtract extract, int extractId, String procedureName) throws Exception {
        long startTime = System.currentTimeMillis();

        EntityManager entityManager = PersistenceManager.getEntityManager();

        System.out.println("Running " + procedureName);

        Integer statusCode = 0;
        String medicationStatus = extract.getParameters().get(0).getStatus();
        if (medicationStatus.equals("activeOnly")) {
            statusCode = 1;
        }

        StoredProcedureQuery query = entityManager.createStoredProcedureQuery(procedureName)
                .registerStoredProcedureParameter("col_list", String.class, ParameterMode.IN)
                .registerStoredProcedureParameter("status_code", Integer.class, ParameterMode.IN)
                .registerStoredProcedureParameter("extractId", Integer.class, ParameterMode.IN)
                .setParameter("col_list", extract.getFields())
                .setParameter("status_code", statusCode)
                .setParameter("extractId", extractId);

        query.execute();
        long estimatedTime = System.currentTimeMillis() - startTime;
        System.out.println(procedureName + " executed, Time taken " + estimatedTime);
    }

    private void runGenericStatusExtractAll(DatasetConfigExtract extract, int extractId, String procedureName,
                                            int codeSetId, int maxTransactionId) throws Exception {
        long startTime = System.currentTimeMillis();
        EntityManager entityManager = PersistenceManager.getEntityManager();

        System.out.println("Running " + procedureName);

        Integer medStatus = 0;
        String medicationStatus = extract.getParameters().get(0).getStatus();
        if (medicationStatus.equals("activeOnly")) {
            medStatus = 1;
        }

        StoredProcedureQuery query = entityManager.createStoredProcedureQuery(procedureName)
                .registerStoredProcedureParameter("extractId", Integer.class, ParameterMode.IN)
                .registerStoredProcedureParameter("status_code", Integer.class, ParameterMode.IN)
                .registerStoredProcedureParameter("codeSetId", Integer.class, ParameterMode.IN)
                .registerStoredProcedureParameter("maxTransactionId", Integer.class, ParameterMode.IN)
                .setParameter("extractId", extractId)
                .setParameter("status_code", medStatus)
                .setParameter("codeSetId", codeSetId)
                .setParameter("maxTransactionId", maxTransactionId);

        query.execute();
        long estimatedTime = System.currentTimeMillis() - startTime;
        System.out.println(procedureName + " executed, Time taken " + estimatedTime);
    }

    private int getMaxTransactionId() throws Exception {
        EntityManager entityManager = PersistenceManager.getEntityManager();

        String sql = "select max(id) from pcr.event_log;";
        try {
            Query query = entityManager.createNativeQuery(sql);

            int results = (int)query.getSingleResult();

            return results;

        } finally {
            entityManager.close();
        }
    }

    private void runFinaliseExtract(int extractId, int maxTransactionId) throws Exception {
        EntityManager entityManager = PersistenceManager.getEntityManager();

        System.out.println("Running finalise_extract" );


        StoredProcedureQuery query = entityManager.createStoredProcedureQuery("finalise_extract")
                .registerStoredProcedureParameter("extractId", Integer.class, ParameterMode.IN)
                .registerStoredProcedureParameter("maxTransactionId", Integer.class, ParameterMode.IN)
                .setParameter("extractId", extractId)
                .setParameter("maxTransactionId", maxTransactionId);

        query.execute();
        System.out.println("finalise_extract executed");
    }
}
