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
import javax.persistence.Query;
import java.io.FileWriter;
import java.util.List;

public class ExtractSQLToCsv implements Job {

    private static final Logger LOG = LoggerFactory.getLogger(ExtractSQLToCsv.class);

    public void execute(JobExecutionContext jobExecutionContext) {

        System.out.println("Extract Temp tables to CSV");

        //TODO Dump Temp tables to CSV
        try {

            DatasetConfig config = DatasetCache.getDatasetConfig(1);
            System.out.println(config.getName());
            processSQLtoCSV(config);
        } catch (Exception e) {
            System.out.println("Error: " + e.getMessage());
        }


        System.out.println("CSV files generated");
    }

    private void processSQLtoCSV(DatasetConfig extractConfig) throws Exception {
        int extractId = extractConfig.getId();
        for (DatasetConfigExtract extract : extractConfig.getExtract()) {
            System.out.println(extract.getType());
            switch (extract.getType()) {
                case "patient" :
                    runSQLtoCSV(extract, extractId, "pat_test p");
                    break;
                case "medication" :
                    runSQLtoCSV(extract, extractId, "med_test m");
                    break;
                case "observation" :
                    runSQLtoCSV(extract, extractId, "obs_test o");
                    break;
                case "allergy" :
                    runSQLtoCSV(extract, extractId, "all_test o");
                    break;
                case "immunisation" :
                    runSQLtoCSV(extract, extractId, "imm_test o");
                    break;
            }
        }
    }

    private void runSQLtoCSV(DatasetConfigExtract config, int extractId, String tableName) throws Exception {

        try {

            String fields = config.getFields();

            // TODO use a white list or some other method to ensure this is safe from SQL injection
            String sql = String.format("select %s from data_generator.%s", fields, tableName);

            boolean headersCreated = false;
            int pageNumber = 1;
            int pageSize = 1000;
            List<Object[]> results = executeSQLBatch(sql, pageNumber++, pageSize);
            while (results.size() > 0) {
                if (!headersCreated) {
                    createCSV(fields, tableName);
                    headersCreated = true;
                }
                saveToCSV(results, tableName);

                System.out.println("getting page " + pageNumber + " of " + tableName);
                results = executeSQLBatch(sql, pageNumber++, pageSize);

            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private List<Object[]> executeSQLBatch(String sql, int pageNumber, int pageSize) throws Exception {

        EntityManager entityManager = PersistenceManager.getEntityManager();

        try {
            Query query = entityManager.createNativeQuery(sql);


            query.setFirstResult((pageNumber - 1) * pageSize);
            query.setMaxResults(pageSize);

            List<Object[]> results = query.getResultList();

            return results;

        } finally {
            entityManager.close();
        }
    }

    private void saveToCSV(List<Object[]> results,  String tableName) throws Exception {
        String filename = "C:\\sftpkey\\" + tableName + ".csv";

        FileWriter fw = new FileWriter(filename, true);
        try {
            for (Object[] result : results) {
                for (Object obj : result) {
                    if (obj != null) {
                        fw.append(obj.toString());
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
}
