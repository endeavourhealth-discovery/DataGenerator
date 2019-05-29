package org.endeavourhealth.cegdatabasefilesender.database;

import org.endeavourhealth.scheduler.models.PersistenceManager;
import javax.persistence.*;
import java.util.Objects;

@Entity
@Table(name = "subscriber_file_sender", schema = "data_generator")
public class SubscriberFileSenderEntity {

    private int subscriberId;
    private String definition;

    @Id
    @Column(name = "subscriber_id")
    public int getSubscriberId() {
        return subscriberId;
    }

    public void setSubscriberId(int subscriberId) {
        this.subscriberId = subscriberId;
    }

    @Basic
    @Column(name = "definition")
    public String getDefinition() {
        return definition;
    }

    public void setDefinition(String definition) {
        this.definition = definition;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SubscriberFileSenderEntity that = (SubscriberFileSenderEntity) o;
        return subscriberId == that.subscriberId &&
                Objects.equals(definition, that.definition);
    }

    @Override
    public int hashCode() {
        return Objects.hash(subscriberId, definition);
    }

    public static SubscriberFileSenderEntity getSubscriberFileSenderEntity(int subscriberId) throws Exception {

        EntityManager entityManager = PersistenceManager.getEntityManager();
        SubscriberFileSenderEntity ret = entityManager.find(SubscriberFileSenderEntity.class, subscriberId);
        entityManager.close();
        return ret;
    }

}
