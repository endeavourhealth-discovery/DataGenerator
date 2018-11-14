package org.endeavourhealth.scheduler.job;

import org.endeavourhealth.scheduler.cache.DatasetCache;
import org.endeavourhealth.scheduler.json.DatasetConfig;
import org.endeavourhealth.scheduler.json.DatasetConfigExtract;
import org.endeavourhealth.scheduler.models.PersistenceJPAConfig;
import org.endeavourhealth.scheduler.models.PersistenceManager;
import org.endeavourhealth.scheduler.models.database.DatasetEntity;
import org.endeavourhealth.scheduler.models.repository.DatasetEntityRepository;
import org.hibernate.Session;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.orm.jpa.LocalContainerEntityManagerFactoryBean;
import org.springframework.transaction.annotation.Transactional;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Query;
import java.io.*;
import java.sql.Connection;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Stream;

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


    public static void main(String args[]) {
        ExtractSQLToCsv e = new ExtractSQLToCsv();
        e.test();
    }

    @Transactional(readOnly = true)
    public void test() {
        ApplicationContext ctx = new AnnotationConfigApplicationContext(PersistenceJPAConfig.class);
        LocalContainerEntityManagerFactoryBean cefb = ctx.getBean(LocalContainerEntityManagerFactoryBean.class);
        EntityManagerFactory emf = cefb.getObject();
        EntityManager entityManager = emf.createEntityManager();

        Query query = entityManager.createNativeQuery("select * from dataset");
        List<Object[]> results = query.getResultList();
        System.out.println("size:"+results.size());

        DatasetEntityRepository datasetEntityRepo = ctx.getBean(DatasetEntityRepository.class);
        streamedCsv(new File("C:\\Temp\\test.csv"), datasetEntityRepo, entityManager);

    }

    @Transactional(readOnly = true)
    public void streamedCsv(File file, DatasetEntityRepository datasetEntityRepo, EntityManager entityManager ) {
        Connection conn = entityManager.unwrap(Session.class).connection();


        try(Stream<DatasetEntity> datasetEntityStream = datasetEntityRepo.streamAll()) {
            PrintWriter out = new PrintWriter(new FileOutputStream(file));
            datasetEntityStream.forEach(rethrowConsumer(datasetEntity -> {
                String line = formLine(datasetEntity);
                out.write(line);
                out.write("\n");
                entityManager.detach(datasetEntity);
            }));
            out.flush();
            out.close();
        } catch (IOException e) {
            LOG.info("Exception occurred " + e.getMessage(), e);
            throw new RuntimeException("Exception occurred while exporting results", e);
        }
    }


    private static String formLine(DatasetEntity entity) {
        return String.join(entity.getDatasetId() +  "," + entity.getDefinition());
    }

    private static <T, E extends Exception> Consumer<T> rethrowConsumer(Consumer_WithExceptions<T, E> consumer) throws E {
        return t -> {
            try {
                consumer.accept(t);
            } catch (Exception exception) {
                throwActualException(exception);
            }
        };
    }

    @SuppressWarnings("unchecked")
    private static <E extends Exception> void throwActualException(Exception exception) throws E {
        throw (E) exception;
    }

    @FunctionalInterface
    private interface Consumer_WithExceptions<T, E extends Exception> {
        void accept(T t) throws E;
    }
}
