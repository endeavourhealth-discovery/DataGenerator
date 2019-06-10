package org.endeavourhealth.cegdatabasefilesender.feedback;

import org.endeavourhealth.scheduler.json.SubscriberFileSenderDefinition.SubscriberFileSenderConfig;
import org.endeavourhealth.scheduler.models.PersistenceManager;

import javax.persistence.EntityManager;
import javax.persistence.Query;
import java.nio.file.Path;

public class FeedbackRepository {

    private final SubscriberFileSenderConfig config;

    private EntityManager entityManager;

    public FeedbackRepository(SubscriberFileSenderConfig config) throws Exception {
        this.config = config;
        entityManager = PersistenceManager.getEntityManager();
    }


    public void process(Path path) throws Exception {

            Query query = entityManager.createQuery("update blah set blah = :1 where id = :2");

            query.setParameter(1, "blah");
            query.setParameter(2, "blah");

            query.executeUpdate();
    }

    public void close() {
        entityManager.close();
    }
}
