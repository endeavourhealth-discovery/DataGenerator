package org.endeavourhealth.scheduler.job;

import org.endeavourhealth.scheduler.cache.DatasetCache;
import org.endeavourhealth.scheduler.json.DatasetConfig;
import org.endeavourhealth.scheduler.json.DatasetConfigExtract;
import org.endeavourhealth.scheduler.models.PersistenceManager;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.persistence.EntityManager;
import javax.persistence.ParameterMode;
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

            DatasetConfig config = DatasetCache.getDatasetConfig(1);
            System.out.println(config.getName());
            processExtracts(config, limitCols);
        } catch (Exception e) {
            System.out.println("Error: " + e.getMessage());
        }

        System.out.println("End of Generate Data");
    }

    private void processExtracts(DatasetConfig extractConfig, boolean limitCols) throws Exception {

        int extractId = extractConfig.getId();
        for (DatasetConfigExtract extract : extractConfig.getExtract()) {
            System.out.println(extract.getType());
            switch (extract.getType()) {
                case "patient" :
                    if (limitCols) {
                        runGenericExtract(extract, extractId, "generate_patient");
                    } else {
                        runGenericExtractAll(extract, extractId, "generate_patient_all_col");
                    }
                    break;
                case "medication" :
                    if (limitCols) {
                        runGenericStatusExtract(extract, extractId, "generate_medication");
                    } else {
                        runGenericStatusExtractAll(extract, extractId, "generate_medication_all_col");
                    }
                    break;
                case "observation" :
                    if (limitCols) {
                        runGenericExtract(extract, extractId, "generate_observation");
                    } else {
                        runGenericExtractAll(extract, extractId, "generate_observation_all_col");
                    }
                    break;
                case "allergy" :
                    if (limitCols) {
                        runGenericStatusExtract(extract, extractId, "generate_allergy");
                    } else {
                        runGenericStatusExtractAll(extract, extractId, "generate_allergy_all_col");
                    }
                    break;
                case "immunisation" :
                    if (limitCols) {
                        runGenericExtract(extract, extractId, "generate_immunisation");
                    } else {
                        runGenericExtractAll(extract, extractId, "generate_immunisation_all_col");
                    }
                    break;
            }
        }

    }

    private void runGenericExtract(DatasetConfigExtract extract, int extractId, String procedureName) throws Exception {
        EntityManager entityManager = PersistenceManager.getEntityManager();

        System.out.println("Running " + procedureName);

        StoredProcedureQuery query = entityManager.createStoredProcedureQuery(procedureName)
                .registerStoredProcedureParameter("col_list", String.class, ParameterMode.IN)
                .registerStoredProcedureParameter("extractId", Integer.class, ParameterMode.IN)
                .setParameter("col_list", extract.getFields())
                .setParameter("extractId", extractId);

        query.execute();
        System.out.println(procedureName + " executed");
    }

    private void runGenericExtractAll(DatasetConfigExtract extract, int extractId, String procedureName) throws Exception {
        EntityManager entityManager = PersistenceManager.getEntityManager();

        System.out.println("Running " + procedureName);

        StoredProcedureQuery query = entityManager.createStoredProcedureQuery(procedureName)
                .registerStoredProcedureParameter("extractId", Integer.class, ParameterMode.IN)
                .setParameter("extractId", extractId);

        query.execute();
        System.out.println(procedureName + " executed");
    }

    private void runGenericStatusExtract(DatasetConfigExtract extract, int extractId, String procedureName) throws Exception {
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
        System.out.println(procedureName + " executed");
    }

    private void runGenericStatusExtractAll(DatasetConfigExtract extract, int extractId, String procedureName) throws Exception {
        EntityManager entityManager = PersistenceManager.getEntityManager();

        System.out.println("Running " + procedureName);

        Integer medStatus = 0;
        String medicationStatus = extract.getParameters().get(0).getStatus();
        if (medicationStatus.equals("activeOnly")) {
            medStatus = 1;
        }

        StoredProcedureQuery query = entityManager.createStoredProcedureQuery(procedureName)
                .registerStoredProcedureParameter("medication_status", Integer.class, ParameterMode.IN)
                .registerStoredProcedureParameter("extractId", Integer.class, ParameterMode.IN)
                .setParameter("medication_status", medStatus)
                .setParameter("extractId", extractId);

        query.execute();
        System.out.println(procedureName + " executed");
    }
}
