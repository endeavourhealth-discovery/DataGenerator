package org.endeavourhealth.scheduler.job;

import org.apache.commons.lang3.StringUtils;
import org.endeavourhealth.common.cache.ObjectMapperPool;
import org.endeavourhealth.scheduler.json.DatasetConfig;
import org.endeavourhealth.scheduler.json.DatasetConfigExtract;
import org.endeavourhealth.scheduler.models.PersistenceManager;
import org.endeavourhealth.scheduler.models.database.DatasetEntity;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.persistence.EntityManager;
import javax.persistence.ParameterMode;
import javax.persistence.StoredProcedureQuery;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GenerateData implements Job {

    private static final Logger LOG = LoggerFactory.getLogger(GenerateData.class);
    private static Map<Integer, DatasetConfig> datasetConfigMap = new HashMap<>();

    public void execute(JobExecutionContext jobExecutionContext) {

        System.out.println("Generate Data");

        //TODO Run the data extractor Stored Proc to move the data into new temporary tables

        try {

            DatasetConfig config = getDatasetConfig(1);
            System.out.println(config.getName());
            processExtracts(config.getExtract());
        } catch (Exception e) {
            System.out.println("Error: " + e.getMessage());
        }

        System.out.println("End of Generate Data");
    }

    private DatasetConfig getDatasetConfig(int datasetId) throws Exception {

        // Check if the config is already in the cache
        DatasetConfig config = datasetConfigMap.get(datasetId);

        if (config == null) {
            // get the config from the DB
            DatasetEntity dataset = DatasetEntity.getDatasetDefinition(1);

            String definition = dataset.getDefinition();
            if (!StringUtils.isEmpty(definition)) {

                // Map config to the Java Class
                config = ObjectMapperPool.getInstance().readValue(definition, DatasetConfig.class);

                datasetConfigMap.put(datasetId, config);
            }
        }

        return config;
    }

    private void processExtracts(List<DatasetConfigExtract> extracts) throws Exception {

        for (DatasetConfigExtract extract : extracts) {
            System.out.println(extract.getType());
            switch (extract.getType()) {
                case "patient" :
                    runPatientExtract(extract);
                    break;
                case "medication" :
                    runMedicationExtract(extract);
                    break;
                case "observation" :
                    runObservationExtract(extract);
                    break;
                case "allergy" :
                    runAllergyExtract(extract);
                    break;
                case "immunisation" :
                    runImmunisationExtract(extract);
                    break;
            }
        }

    }

    private void runPatientExtract(DatasetConfigExtract extract) throws Exception {
        /*EntityManager entityManager = PersistenceManager.getEntityManager();

        Integer medStatus = 0;
        String medicationStatus = extract.getParameters().get(0).getStatus();
        if (medicationStatus.equals("activeOnly")) {
            medStatus = 1;
        }

        StoredProcedureQuery query = entityManager.createStoredProcedureQuery("generate_medication")
                .registerStoredProcedureParameter("col_list", String.class, ParameterMode.IN)
                .registerStoredProcedureParameter("medication_status", Integer.class, ParameterMode.IN)
                .setParameter("col_list", extract.getFields())
                .setParameter("medication_status", medStatus);

        query.execute();*/
    }

    private void runMedicationExtract(DatasetConfigExtract extract) throws Exception {
        EntityManager entityManager = PersistenceManager.getEntityManager();

        System.out.println("Running medication extract");

        Integer medStatus = 0;
        String medicationStatus = extract.getParameters().get(0).getStatus();
        if (medicationStatus.equals("activeOnly")) {
            medStatus = 1;
        }

        StoredProcedureQuery query = entityManager.createStoredProcedureQuery("generate_medication")
                .registerStoredProcedureParameter("col_list", String.class, ParameterMode.IN)
                .registerStoredProcedureParameter("medication_status", Integer.class, ParameterMode.IN)
                .setParameter("col_list", extract.getFields())
                .setParameter("medication_status", medStatus);

        query.execute();
        System.out.println("Medication executed");
    }

    private void runObservationExtract(DatasetConfigExtract extract) throws Exception {

    }

    private void runAllergyExtract(DatasetConfigExtract extract) throws Exception {

    }

    private void runImmunisationExtract(DatasetConfigExtract extract) throws Exception {

    }
}
