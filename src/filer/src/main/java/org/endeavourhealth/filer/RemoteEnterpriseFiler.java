package org.endeavourhealth.filer;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Strings;
import com.google.common.io.Files;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.io.FileUtils;
import org.endeavourhealth.common.cache.ObjectMapperPool;
import org.endeavourhealth.core.database.rdbms.ConnectionManager;
import org.endeavourhealth.filer.util.FilerUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.math.BigDecimal;
import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public class RemoteEnterpriseFiler {

    private static final Logger LOG = LoggerFactory.getLogger(RemoteEnterpriseFiler.class);

    private static final String COLUMN_CLASS_MAPPINGS = "ColumnClassMappings.json";
    private static final String DATE_FORMAT = "yyyy-MM-dd";
    private static final String DATE_TIME_FORMAT = "yyyy-MM-dd kk:mm:ss";
    private static final CSVFormat CSV_FORMAT = CSVFormat.DEFAULT;

    private static final String COL_SAVE_MODE = "save_mode";
    private static final String COL_ID = "id";

    private static final String UPSERT = "Upsert";
    private static final String DELETE = "Delete";

    private static final int UPSERT_ATTEMPTS = 15;

    //private static final String ZIP_ENTRY = "EnterpriseData.xml";

    //private static String keywordEscapeChar = null; //different DBs use different chars to escape keywords (" on pg, ` on mysql)

    private static Map<String, HikariDataSource> connectionPools = new ConcurrentHashMap<>();
    private static Map<String, String> escapeCharacters = new ConcurrentHashMap<>();
    private static Map<String, Integer> batchSizes = new ConcurrentHashMap<>();

    private static final ArrayList<String> nonUniqueIdTables =
            new ArrayList<>(Arrays.asList(
                    "patient_additional",
                    "encounter_additional",
                    "patient_uprn"
            ));

    public static void file(String name, String failureDir, Properties properties,
                             String keywordEscapeChar, int batchSize, byte[] bytes,
                            Map<String, ArrayList> columnsMap, Map<String, ArrayList> pksMap,
                            Map<String, Boolean> identityTables) throws Exception {

        ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
        ZipInputStream zis = new ZipInputStream(bais);
        Connection connection = FilerUtil.getConnection(properties);
        connection.setAutoCommit(false);

        try {

            List<DeleteWrapper> deletes = new ArrayList<>();
            JsonNode columnClassMappings = null;

            while (true) {
                ZipEntry entry = zis.getNextEntry();
                if (entry == null) {
                    break;
                }

                String entryFileName = entry.getName();
                byte[] entryBytes = readZipEntry(zis);

                if (entryFileName.equals(COLUMN_CLASS_MAPPINGS)) {
                    String jsonStr = new String(entryBytes);
                    columnClassMappings = ObjectMapperPool.getInstance().readTree(jsonStr);

                } else {
                    processCsvData(entryFileName, entryBytes, columnClassMappings, connection, keywordEscapeChar,
                            batchSize, deletes, columnsMap, pksMap, identityTables);
                }
            }

            //now files the deletes
            fileDeletes(deletes, connection);

        } catch (Exception ex) {
            //if we get an exception, write out the zip file, so we can investigate what was being filed
            //writeZipFile(bytes);

            throw new Exception(ex.getMessage(), ex);
        } finally {
            if (zis != null) {
                zis.close();
            }
            connection.close();
        }
    }



    private static void writeZipFile(byte[] bytes) {
        File f = new File("EnterpriseFileError.zip");
        try {
            FileUtils.writeByteArrayToFile(f, bytes);
            LOG.error("Written ZIP file to " + f);

        } catch (IOException ex) {
            LOG.error("Failed to write ZIP file to " + f, ex);
        }
    }

    private static void fileDeletes(List<DeleteWrapper> deletes, Connection connection) throws Exception {

        String currentTable = null;
        List<CSVRecord> currentRecords = null;
        List<String> currentColumns = null;
        Map<String, Class> currentColumnClasses = null;

        //LOG.trace("Got " + deletes.size() + " deletes");

        //go backwards, so we delete dependent records first
        for (int i=deletes.size()-1; i>=0; i--) {
            DeleteWrapper wrapper = deletes.get(i);
            String tableName = wrapper.getTableName();

            if (currentTable == null
                    || !currentTable.equals(tableName)) {

                //file any deletes we've already built up
                if (currentRecords != null
                        && !currentRecords.isEmpty()) {
                    LOG.trace("Deleting " + currentRecords.size() + " from " + currentTable);
                    fileDeletes(currentRecords, currentColumns, currentColumnClasses, currentTable, connection);
                }

                currentTable = tableName;
                currentRecords = new ArrayList<>();
                currentColumns = wrapper.getColumns();
                currentColumnClasses = wrapper.getColumnClasses();
            }

            currentRecords.add(wrapper.getRecord());
        }

        //file any deletes we've already built up
        if (currentRecords != null
                && !currentRecords.isEmpty()) {
            LOG.trace("Deleting " + currentRecords.size() + " from " + currentTable);
            fileDeletes(currentRecords, currentColumns, currentColumnClasses, currentTable, connection);
        }
    }

    private static byte[] readZipEntry(ZipInputStream zis) throws Exception {

        byte[] buffer = new byte[2048];

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        BufferedOutputStream bos = new BufferedOutputStream(baos, buffer.length);

        int len = 0;
        while ((len = zis.read(buffer)) > 0) {
            bos.write(buffer, 0, len);
        }

        bos.flush();
        bos.close();

        return baos.toByteArray();
    }

    private static ArrayList<String> getTableColumns(Connection connection, String tableName) throws Exception{
        LOG.info("Getting columns for table:" + tableName);
        ArrayList<String> columns = new ArrayList<>();
        String sql = null;
        if (ConnectionManager.isSqlServer(connection) || ConnectionManager.isPostgreSQL(connection)) {
            sql = "select column_name as field from information_schema.columns where table_name = '" + tableName + "';";
        } else {
            sql = "describe " + tableName + ";";
        }
        PreparedStatement ps = connection.prepareStatement(sql);
        ps.executeQuery();
        ResultSet resultSet = ps.getResultSet();
        while (resultSet.next()) {
            columns.add(resultSet.getString("field"));
        }
        resultSet.close();
        ps.close();
        return columns;
    }

    private static void processCsvData(String entryFileName, byte[] csvBytes, JsonNode allColumnClassMappings,
                                       Connection connection, String keywordEscapeChar, int batchSize,
                                       List<DeleteWrapper> deletes, Map<String, ArrayList> columnsMap,
                                       Map<String, ArrayList> pksMap, Map<String, Boolean> identityTables) throws Exception {

        String tableName = Files.getNameWithoutExtension(entryFileName);
        ArrayList<String> actualColumns = columnsMap.get(tableName);
        if (actualColumns == null) {
            actualColumns = getTableColumns(connection, tableName);
            columnsMap.put(tableName, actualColumns);
        }

        ByteArrayInputStream bais = new ByteArrayInputStream(csvBytes);
        InputStreamReader isr = new InputStreamReader(bais);
        CSVParser csvParser = new CSVParser(isr, CSV_FORMAT.withHeader());

        ArrayList<String> removeCols = new ArrayList<>();
        Map<String, Integer> csvHeaderMap = csvParser.getHeaderMap();
        for (String column : csvHeaderMap.keySet()) {
            if (!column.equals(COL_SAVE_MODE)) {
                if (!actualColumns.contains(column)) {
                    removeCols.add(column);
                }
            }
        }

        for (String column : removeCols) {
            LOG.debug("Removing column:" + column + " from the header map for table:" + tableName);
            csvHeaderMap.remove(column);
        }

        //find out what columns we've got
        List<String> columns = new ArrayList<>();
        HashMap<String, Class> columnClasses = new HashMap<>();
        createHeaderColumnMap(csvHeaderMap, entryFileName, allColumnClassMappings, columns, columnClasses);


        //since we're dealing with small volumes, we can just read keep all the records in memory
        List<CSVRecord> upserts = new ArrayList<>();
        //List<CSVRecord> deletes = new ArrayList<>();

        Iterator<CSVRecord> csvIterator = csvParser.iterator();
        while (csvIterator.hasNext()) {
            CSVRecord csvRecord = csvIterator.next();
            String saveMode = csvRecord.get(COL_SAVE_MODE);

            if (saveMode.equalsIgnoreCase(DELETE)) {
                //we have to play deletes in reverse, so just add to this list and they'll be processed after all the upserts
                deletes.add(new DeleteWrapper(tableName, csvRecord, columns, columnClasses));

            } else if (saveMode.equalsIgnoreCase(UPSERT)) {
                upserts.add(csvRecord);

            } else {
                throw new Exception("Unknown save mode " + saveMode);
            }
        }

        //when doing a bulk, we can have 300,000+ practitioners, so do them in batches, so we're
        //not keeping huge DB transactions open
        List<CSVRecord> batch = new ArrayList<>();
        for (CSVRecord record: upserts) {
            batch.add(record);

            //in testing, batches of 20000 seemed best, although there wasn't much difference between batches of 5000 up to 100000
            if (batch.size() >= batchSize) {
                fileUpsertsWithRetry(batch, columns, columnClasses, tableName, connection, keywordEscapeChar, pksMap, identityTables);
                batch = new ArrayList<>();
            }
        }
        if (!batch.isEmpty()) {
            fileUpsertsWithRetry(batch, columns, columnClasses, tableName, connection, keywordEscapeChar, pksMap, identityTables);
        }
    }

    private static void createHeaderColumnMap(Map<String, Integer> csvHeaderMap,
                                                String entryFileName,
                                                JsonNode allColumnClassMappings,
                                                List<String> columns,
                                                HashMap<String, Class> columnClasses) throws Exception {
        //get the column names, ordered
        String[] arr = new String[csvHeaderMap.size()];
        for (Map.Entry<String, Integer> entry : csvHeaderMap.entrySet())
            arr[entry.getValue()] = entry.getKey();

        for (String column: arr)
            columns.add(column);

        //sort out column classes
        JsonNode columnClassMappings = allColumnClassMappings.get(entryFileName);

        for (String column: csvHeaderMap.keySet()) {
            JsonNode node = columnClassMappings.get(column);
            if (node == null) {
                throw new Exception("Failed to find class for column " + column + " in " + entryFileName);
            }
            String className = node.asText();
            Class cls = null;

            //getting the name of the primative types returns "int", but
            //this doesn't work in reverse, trying to get them using Class.forName("int")
            //so we have to manually check for these primative types
            if (className.equals("int")) {
                cls = Integer.TYPE;
            } else if (className.equals("long")) {
                cls = Long.TYPE;
            } else if (className.equals("boolean")) {
                cls = Boolean.TYPE;
            } else {
                cls = Class.forName(className);
            }

            columnClasses.put(column, cls);
        }
    }

    private static void fileDeletes(List<CSVRecord> csvRecords, List<String> columns, Map<String, Class> columnClasses,
                                    String tableName, Connection connection) throws Exception {

        if (csvRecords.isEmpty()) {
            return;
        }

        PreparedStatement delete = connection.prepareStatement("delete from " + tableName + " where id = ?");

        for (CSVRecord csvRecord: csvRecords) {

            addToStatement(delete, csvRecord, COL_ID, columnClasses, 1);
            delete.addBatch();
        }

        delete.executeBatch();
        delete.close(); //was forgetting to do this
        connection.commit();
    }

    private static void addToStatement(PreparedStatement statement, CSVRecord csvRecord, String column, Map<String, Class> columnClasses, int index) throws Exception {

        String value = csvRecord.get(column);
        Class fieldCls = columnClasses.get(column);

        //hack to get around the UUIDs that have been used as proxy Cerner codes. Lets us file the data in the short term.
        if (column.equals("original_code")
                && !Strings.isNullOrEmpty(value)
                && value.length() > 20) {
            value = value.substring(0, 20);
        }

        if (fieldCls == String.class) {
            if (Strings.isNullOrEmpty(value)) {
                statement.setNull(index, Types.VARCHAR);
            } else {
                statement.setString(index, (String)value);
            }

        } else if (fieldCls == Date.class) {
            if (Strings.isNullOrEmpty(value)) {
                statement.setNull(index, Types.DATE);
            } else {
                //Date d = new SimpleDateFormat(DATE_FORMAT).parse(value);
                //statement.setTimestamp(index, new Timestamp(d.getTime()));
                Date d = null;
                try {
                    d = new SimpleDateFormat(DATE_TIME_FORMAT).parse(value);
                } catch (ParseException e) {
                    d = new SimpleDateFormat(DATE_FORMAT).parse(value);
                }
                statement.setTimestamp(index, new Timestamp(d.getTime()));
            }

        } else if (fieldCls == BigDecimal.class) {
            if (Strings.isNullOrEmpty(value)) {
                statement.setNull(index, Types.DECIMAL);
            } else {
                BigDecimal bd = new BigDecimal(value);
                statement.setBigDecimal(index, bd);
            }

        } else if (fieldCls == Integer.class
            || fieldCls == Integer.TYPE) {

            if (Strings.isNullOrEmpty(value)) {
                statement.setNull(index, Types.INTEGER);
            } else {
                int i = Integer.parseInt(value);
                statement.setInt(index, i);
            }

        } else if (fieldCls == Long.class
            || fieldCls == Long.TYPE) {

            if (Strings.isNullOrEmpty(value)) {
                statement.setNull(index, Types.BIGINT);
            } else {
                long l = Long.parseLong(value);
                statement.setLong(index, l);
            }

        } else if (fieldCls == Boolean.class
            || fieldCls == Boolean.TYPE) {

            //explicitly test for expected values rather than using Boolean.parseBoolean(..) so anything
            //not expected (e.g. 1 or 0) is treated as an error
            if (Strings.isNullOrEmpty(value)) {
                statement.setNull(index, Types.BOOLEAN);

            } else if (value.equalsIgnoreCase("true")
                    || value.equalsIgnoreCase("1")) {
                statement.setBoolean(index, true);

            } else if (value.equalsIgnoreCase("false")
                    || value.equalsIgnoreCase("0")) {
                statement.setBoolean(index, false);

            } else {
                throw new Exception("Unexpected boolean value [" + value + "] in column [" + column + "]");
            }

        } else {
            throw new Exception("Unsupported value class " + fieldCls);
        }
    }

    private static ArrayList<String> getSQLTablePKs(String tableName, Connection connection) throws Exception {
        ArrayList<String> pks = new ArrayList<>();
        String sql = "SELECT column_name as primary_key " +
                "     FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS TC " +
                "     INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS KU " +
                "     ON TC.CONSTRAINT_TYPE = 'PRIMARY KEY' " +
                "     AND TC.CONSTRAINT_NAME = KU.CONSTRAINT_NAME " +
                "     AND KU.table_name='" + tableName +"' " +
                "     ORDER BY KU.TABLE_NAME, KU.ORDINAL_POSITION;";

        PreparedStatement ps = connection.prepareStatement(sql);
        ps.executeQuery();
        ResultSet resultSet = ps.getResultSet();
        while (resultSet.next()) {
            pks.add(resultSet.getString("primary_key"));
        }
        resultSet.close();
        ps.close();
        return pks;
    }

    private static boolean checkForIdentityTable(String tableName, Connection connection) throws Exception {
        int count = 0;
        String sql = "SELECT count(*) as value " +
                "FROM sys.identity_columns " +
                "WHERE OBJECT_NAME(object_id) = '"+ tableName +"';";
        PreparedStatement ps = connection.prepareStatement(sql);
        ps.executeQuery();
        ResultSet resultSet = ps.getResultSet();
        while (resultSet.next()) {
            count = resultSet.getInt("value");
        }
        resultSet.close();
        ps.close();
        return (count != 0);
    }

    private static PreparedStatement createUpsertPreparedStatement(String tableName, List<String> columns,
                                                                   Connection connection, String keywordEscapeChar,
                                                                   Map<String, ArrayList> pksMap) throws Exception {

        if (ConnectionManager.isSqlServer(connection)) {

            StringBuilder sql = new StringBuilder();
            if (nonUniqueIdTables.contains(tableName)) {

                ArrayList<String> pks = pksMap.get(tableName);
                if (pks == null) {
                    pks = getSQLTablePKs(tableName, connection);
                    pksMap.put(tableName, pks);
                }

                /*
                MERGE INTO organization_additional AS t USING
                    (SELECT id=?, property_id=?, value_id=?, json_value=?, value=?,name=?) AS s
                  ON t.id = s.id
                  AND t.property_id = s.property_id
                  AND t.value_id = s.value_id
                  WHEN MATCHED THEN
                    UPDATE SET id=s.id, property_id =s.property_id, value_id=s.value_id, json_value=s.json_value, value=s.value, name=s.name
                  WHEN NOT MATCHED THEN
                    INSERT (id, property_id , value_id, json_value, value, name)
                    VALUES (s.id, s.property_id, s.value_id, s.json_value, s.value, s.name);
                 */
                sql.append("MERGE INTO " + tableName + " AS t USING (SELECT ");
                for (int i = 0; i < columns.size() - 1; i++) {
                    String column = columns.get(i);
                    sql.append(column + "=?,");
                }
                sql.append(columns.get(columns.size() - 1) + "=?) AS s ");
                sql.append("ON t." + pks.get(0) + "= s." + pks.get(0) + " ");
                for (int i = 1; i < pks.size(); i++) {
                    sql.append("AND t." + pks.get(i) + "= s." + pks.get(i) + " ");
                }
                sql.append("WHEN MATCHED THEN UPDATE SET ");
                for (int i = 0; i < columns.size() - 1; i++) {
                    String column = columns.get(i);
                    if (!column.equalsIgnoreCase(COL_ID)) {
                        sql.append(column + "=s." + column + ",");
                    }
                }
                sql.append(columns.get(columns.size() - 1) + "=s." + columns.get(columns.size() - 1) + " ");
                sql.append("WHEN NOT MATCHED THEN INSERT ( ");
                for (int i = 0; i < columns.size() - 1; i++) {
                    String column = columns.get(i);
                    sql.append(column + ",");
                }
                sql.append(columns.get(columns.size() - 1) + ") VALUES (");
                for (int i = 0; i < columns.size() - 1; i++) {
                    String column = columns.get(i);
                    sql.append("s." + column + ",");
                }
                sql.append("s." + columns.get(columns.size() - 1) + "); ");

            } else {
                //keywordEscapeChar = "";

                /*
                SQL Server doesn't support upserts in a single statement, so we need to handle this with an attempted update
                and then an insert if that didn't work

                 e.g.
                UPDATE dbo.AccountDetails
                SET Etc = @Etc
                WHERE Email = @Email

                INSERT dbo.AccountDetails ( Email, Etc )
                SELECT @Email, @Etc
                WHERE @@ROWCOUNT=0
                 */

                //first write out the prepared statement for the UPDATE
                //the columns are written to the prepared statement in the order supplied, meaning ID is
                //always first. So we need to format this prepared statement in such a way that we can accept it
                //as the first parameter, even though syntax means it is needed last
                if ("link_distributor".equals(tableName)) {
                    sql.append("DECLARE @id_tmp varchar(255);");
                    sql.append("SET @id_tmp = ?;");
                } else {
                    sql.append("DECLARE @id_tmp bigint;");
                    sql.append("SET @id_tmp = ?;");
                }
                sql.append("UPDATE " + tableName + " SET ");

                for (int i = 0; i < columns.size(); i++) {
                    String column = columns.get(i);
                    if (column.equals(COL_ID)) {
                        continue;
                    }

                    sql.append(keywordEscapeChar + column + keywordEscapeChar + " = ?");
                    if (i + 1 < columns.size()) {
                        sql.append(", ");
                    }
                }
                if ("link_distributor".equals(tableName)) {
                    sql.append(" WHERE " + keywordEscapeChar + "source_skid" + keywordEscapeChar + " = @id_tmp;");
                } else {
                    sql.append(" WHERE " + keywordEscapeChar + COL_ID + keywordEscapeChar + " = @id_tmp;");
                }
                //then write out SQL for an insert to run if the above update affected zero rows
                sql.append("INSERT INTO " + tableName + "(");

                for (int i = 0; i < columns.size(); i++) {
                    String column = columns.get(i);
                    sql.append(keywordEscapeChar + column + keywordEscapeChar);
                    if (i + 1 < columns.size()) {
                        sql.append(", ");
                    }
                }

                sql.append(") SELECT ");

                for (int i = 0; i < columns.size(); i++) {
                    sql.append("?");
                    if (i + 1 < columns.size()) {
                        sql.append(", ");
                    }
                }

                sql.append(" WHERE @@ROWCOUNT=0;");
            }
            return connection.prepareStatement(sql.toString());

        } else if (ConnectionManager.isPostgreSQL(connection)) {

            StringBuilder sql = new StringBuilder();
            sql.append("INSERT INTO " + tableName + " (");

            for (int i = 0; i < columns.size(); i++) {
                String column = columns.get(i);
                sql.append(keywordEscapeChar + column + keywordEscapeChar);
                if (i + 1 < columns.size()) {
                    sql.append(", ");
                }
            }

            sql.append(") VALUES (");

            for (int i = 0; i < columns.size(); i++) {
                sql.append("?");
                if (i + 1 < columns.size()) {
                    sql.append(", ");
                }
            }

            sql.append(") ON CONFLICT (");
            sql.append(COL_ID);
            sql.append(") DO UPDATE SET ");

            for (int i = 0; i < columns.size(); i++) {
                String column = columns.get(i);
                if (column.equals(COL_ID)) {
                    continue;
                }

                sql.append(keywordEscapeChar + column + keywordEscapeChar + " = EXCLUDED." + keywordEscapeChar + column + keywordEscapeChar);
                if (i + 1 < columns.size()) {
                    sql.append(", ");
                }
            }

            sql.append(";");

            return connection.prepareStatement(sql.toString());

        } else {

            StringBuilder sql = new StringBuilder();
            sql.append("INSERT INTO " + tableName + "(");

            for (int i = 0; i < columns.size(); i++) {
                String column = columns.get(i);
                sql.append(keywordEscapeChar + column + keywordEscapeChar);
                if (i + 1 < columns.size()) {
                    sql.append(", ");
                }
            }

            sql.append(") VALUES (");

            for (int i = 0; i < columns.size(); i++) {
                sql.append("?");
                if (i + 1 < columns.size()) {
                    sql.append(", ");
                }
            }

            sql.append(") ON DUPLICATE KEY UPDATE ");

            for (int i = 0; i < columns.size(); i++) {
                String column = columns.get(i);
                if (column.equals(COL_ID)) {
                    continue;
                }

                sql.append(keywordEscapeChar + column + keywordEscapeChar + " = VALUES(" + keywordEscapeChar + column + keywordEscapeChar + ")");
                if (i + 1 < columns.size()) {
                    sql.append(", ");
                }
            }

            sql.append(";");

            return connection.prepareStatement(sql.toString());
        }
    }

    /**
     * When multiple subscriber queue readers are writing to the same subscriber DB, we regularly see one of them
     * killed due to a deadlock. This seems to be a transient issue, so backing off for a few seconds and trying
     * again should mitigate it
     */
    private static void fileUpsertsWithRetry(List<CSVRecord> csvRecords, List<String> columns, HashMap<String, Class> columnClasses,
                                             String tableName, Connection connection, String keywordEscapeChar,
                                             Map<String, ArrayList> pksMap, Map<String, Boolean> identityTables) throws Exception {

        int attemptsMade = 0;
        while (true) {

            try {
                fileUpserts(csvRecords, columns, columnClasses, tableName, connection, keywordEscapeChar, pksMap, identityTables);

                //if we execute without error, break out
                break;

            } catch (BatchUpdateException ex) {
                String msg = ex.getMessage();
                if (attemptsMade < UPSERT_ATTEMPTS
                        && msg != null
                        && (msg.startsWith("Deadlock found when trying to get lock; try restarting transaction")
                            || msg.startsWith("Lock wait timeout exceeded; try restarting transaction"))) {

                    //if the message matches the deadlock one, then wait a while and try again
                    attemptsMade ++;
                    long delay = getMsDelayForRetry(attemptsMade);
                    LOG.error("Upsert to " + tableName + " failed due to deadlock, so will try again in " + (delay/1000L) + " seconds");
                    Thread.sleep(delay);
                    continue;

                } else {
                    //log the records we failed on
                    LOG.error("Failed on batch:");
                    try {
                        LOG.error("" + String.join(", ", columns));
                        for (CSVRecord record : csvRecords) {
                            List<String> recordList = new ArrayList<>();
                            for (String col : columns) {
                                String val = record.get(col);
                                recordList.add(val);
                            }
                            LOG.error("" + String.join(", ", recordList));
                        }
                    } catch (Throwable t) {
                        LOG.error("ERROR LOGGING FAILED BATCH", t);
                    }

                    //if the message isn't exactly the one we're looking for, just throw the exception as normal
                    throw ex;
                }
            }

        }
    }

    private static long getMsDelayForRetry(int attemptNumber) {
        double d = Math.exp(attemptNumber);
        return (long)(d * 1000D);
    }

    private static void fileUpserts(List<CSVRecord> csvRecords, List<String> columns, HashMap<String, Class> columnClasses,
                                    String tableName, Connection connection, String keywordEscapeChar,
                                    Map<String, ArrayList> pksMap, Map<String, Boolean> identityTables) throws Exception {

        if (csvRecords.isEmpty()) {
            return;
        }

        //the first element in the columns list is the save mode, so remove that
        columns = new ArrayList<>(columns);
        columns.remove(COL_SAVE_MODE);

        PreparedStatement insert = null;

        try {
            insert = createUpsertPreparedStatement(tableName, columns, connection, keywordEscapeChar, pksMap);

            int index = 1;
            if ("link_distributor".equals(tableName)) {
                addToStatement(insert, csvRecords.get(0), columns.get(0), columnClasses, index);
            } else {

            }

            for (CSVRecord csvRecord: csvRecords) {
                if ("link_distributor".equals(tableName)) {
                    index = 2;
                } else {
                    index = 1;
                }
                for (String column: columns) {
                    addToStatement(insert, csvRecord, column, columnClasses, index);
                    index ++;
                }

                //if SQL Server, then we need to add the values a SECOND time because the UPSERT syntax used needs it
                //Skip if it the table is in nonUniqueIdTables (it uses MERGE syntax)
                if (ConnectionManager.isSqlServer(connection) &&
                        !nonUniqueIdTables.contains(tableName)) {
                    for (String column: columns) {
                        addToStatement(insert, csvRecord, column, columnClasses, index);
                        index ++;
                    }
                }

                /*if (!tableName.equals("link_distributor")) {
                    LOG.debug("Saving " + tableName + " with ID " + csvRecord.get(COL_ID));
                }*/

                insert.addBatch();
            }

            insert.executeBatch();

            connection.commit();

        } catch (Exception ex) {

            connection.rollback();

            //if we get an error, try again, but one record at a time so we can find out exactly which record caused the problem
            if (csvRecords.size() == 1) {
                LOG.error(insert.toString());
                throw ex;
            } else {
                LOG.error("Had exception saving batch (" + ex.getMessage() + " so will try saving one at a time");
                for (CSVRecord r: csvRecords) {
                    List<CSVRecord> l = new ArrayList<>();
                    l.add(r);
                    fileUpserts(l, columns, columnClasses, tableName, connection, keywordEscapeChar, pksMap, identityTables);
                }
            }
        } finally {
            if (insert != null) {
                insert.close();
            }
        }

    }



    /*private static int getBatchSize(String url) {
        return batchSizes.get(url).intValue();
    }

    private static String getKeywordEscapeChar(String url) {
        return escapeCharacters.get(url);
    }*/



    /*public static Connection openConnection(JsonNode config) throws Exception {

        String url = config.get("enterprise_url").asText();
        HikariDataSource pool = connectionPools.get(url);
        if (pool == null) {
            synchronized (connectionPools) {
                pool = connectionPools.get(url);
                if (pool == null) {

                    String driverClass = config.get("driverClass").asText();
                    String username = config.get("enterprise_username").asText();
                    String password = config.get("enterprise_password").asText();

                    //force the driver to be loaded
                    Class.forName(driverClass);

                    pool = new HikariDataSource();
                    pool.setJdbcUrl(url);
                    pool.setUsername(username);
                    pool.setPassword(password);
                    pool.setMaximumPoolSize(3);
                    pool.setMinimumIdle(1);
                    pool.setIdleTimeout(60000);
                    pool.setPoolName("EnterpriseFilerConnectionPool" + url);
                    pool.setAutoCommit(false);

                    connectionPools.put(url, pool);

                    //cache the escape string too, since getting the metadata each time is extra load
                    Connection conn = pool.getConnection();
                    String escapeStr = conn.getMetaData().getIdentifierQuoteString();
                    escapeCharacters.put(url, escapeStr);
                    conn.close();

                    //and catch the batch size
                    int batchSize = 50;
                    if (config.has("batch_size")) {
                        batchSize = config.get("batch_size").asInt();
                        if (batchSize <= 0) {
                            throw new Exception("Invalid batch size");
                        }
                    }
                    batchSizes.put(url, new Integer(batchSize));
                }
            }
        }

        return pool.getConnection();
    }*/



}

