package org.endeavourhealth.cegdatabasefilesender.feedback;

import org.endeavourhealth.cegdatabasefilesender.feedback.bean.FailureResult;
import org.endeavourhealth.cegdatabasefilesender.feedback.bean.SuccessResult;
import org.endeavourhealth.scheduler.json.SubscriberFileSenderDefinition.SubscriberFileSenderConfig;
import org.endeavourhealth.scheduler.models.PersistenceManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.persistence.EntityManager;
import javax.persistence.Query;

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

    public void process(SuccessResult successResult) {

        logger.info("Processing {}", successResult);

//        Query query = entityManager.createQuery("update blah set blah = :1 where id = :2");
//
//        query.setParameter(1, "blah");
//        query.setParameter(2, "blah");
//
//        query.executeUpdate();
    }

    public void process(FailureResult failureResult) {

        logger.info("Processing {}", failureResult);

//        Query query = entityManager.createQuery("update blah set blah = :1 where id = :2");
//
//        query.setParameter(1, "blah");
//        query.setParameter(2, "blah");
//
//        query.executeUpdate();
    }
}
