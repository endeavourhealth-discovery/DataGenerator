package org.endeavourhealth.cegdatabasefilesender.feedback;

import org.endeavourhealth.cegdatabasefilesender.feedback.bean.FailureResult;
import org.endeavourhealth.cegdatabasefilesender.feedback.bean.SuccessResult;
import org.endeavourhealth.scheduler.json.SubscriberFileSenderDefinition.SubscriberFileSenderConfig;
import org.endeavourhealth.scheduler.models.PersistenceManager;
import org.hibernate.internal.SessionImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import javax.persistence.EntityManager;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Timestamp;

public class FeedbackRepository {

    private static final Logger logger = LoggerFactory.getLogger(FeedbackRepository.class);

    private final SubscriberFileSenderConfig config;

    private EntityManager entityManager;

    public FeedbackRepository(SubscriberFileSenderConfig config) throws Exception {
        this.config = config;
        entityManager = PersistenceManager.getEntityManager();
    }

    public void close() {
        entityManager.close();
    }

    public void process(SuccessResult successResult) throws Exception {

        logger.info("Processing {}", successResult);

        String successResultUuid = successResult.getUuid();

        EntityManager entityManager = PersistenceManager.getEntityManager();
        PreparedStatement ps = null;

        try {
            entityManager.getTransaction().begin();
            SessionImpl session = (SessionImpl) entityManager.getDelegate();
            Connection connection = session.connection();

            java.util.Date date = new java.util.Date();
            Timestamp timestamp = new java.sql.Timestamp(date.getTime());

            String sql = "update data_generator.subscriber_zip_file_uuids"
                    + " set file_filing_attempted = ?"
                    + ", file_filing_success = true"
                    + ", queued_message_body = null"
                    + " where queued_message_uuid = ?";

            ps = connection.prepareStatement(sql);
            ps.setTimestamp(1, timestamp);
            ps.setString(2, successResultUuid);
            ps.executeUpdate();
            entityManager.getTransaction().commit();

        } catch (Exception ex) {
            entityManager.getTransaction().rollback();
            throw ex;

        } finally {
            if (ps != null) {
                ps.close();
            }
            entityManager.close();
        }

    }

    public void process(FailureResult failureResult) throws Exception{

        logger.info("Processing {}", failureResult);

        String failureResultUuid = failureResult.getUuid();
        String failureResultMessage = failureResult.getMessage();

        EntityManager entityManager = PersistenceManager.getEntityManager();
        PreparedStatement ps = null;

        try {
            entityManager.getTransaction().begin();
            SessionImpl session = (SessionImpl) entityManager.getDelegate();
            Connection connection = session.connection();

            java.util.Date date = new java.util.Date();
            Timestamp timestamp = new java.sql.Timestamp(date.getTime());

            String sql = "update data_generator.subscriber_zip_file_uuids"
                    + " set file_filing_attempted = ?"
                    + ", file_filing_success = false"
                    + ", filing_failure_message = ?"
                    + " where queued_message_uuid = ?";

            ps = connection.prepareStatement(sql);
            ps.setTimestamp(1, timestamp);
            ps.setString(2, failureResultMessage);
            ps.setString(3, failureResultUuid);
            ps.executeUpdate();
            entityManager.getTransaction().commit();

        } catch (Exception ex) {
            entityManager.getTransaction().rollback();
            throw ex;

        } finally {
            if (ps != null) {
                ps.close();
            }
            entityManager.close();
        }

    }

}
