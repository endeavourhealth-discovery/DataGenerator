package org.endeavourhealth.cohortmanager.models;

import org.endeavourhealth.cohortmanager.querydocument.QueryDocumentSerializer;
import org.endeavourhealth.cohortmanager.querydocument.models.LibraryItem;
import org.endeavourhealth.scheduler.models.PersistenceManager;

import javax.persistence.*;

/**
 * Created by darren on 06/12/2018.
 */
@Entity
@Table(name = "cohort", schema = "data_generator", catalog = "")
public class CohortEntityDG {
    private int id;
    private String title;
    private String xmlContent;

    @Id
    @Column(name = "id", nullable = false)
    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    @Basic
    @Column(name = "title", nullable = false, length = 255)
    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    @Basic
    @Column(name = "xml_content", nullable = false, length = -1)
    public String getXmlContent() {
        return xmlContent;
    }

    public void setXmlContent(String xmlContent) {
        this.xmlContent = xmlContent;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        CohortEntityDG that = (CohortEntityDG) o;

        if (id != that.id) return false;
        if (title != null ? !title.equals(that.title) : that.title != null) return false;
        if (xmlContent != null ? !xmlContent.equals(that.xmlContent) : that.xmlContent != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = id;
        result = 31 * result + (title != null ? title.hashCode() : 0);
        result = 31 * result + (xmlContent != null ? xmlContent.hashCode() : 0);
        return result;
    }

    public static LibraryItem getCohort(Integer id) throws Exception {
        EntityManager entityManager = PersistenceManager.getEntityManager();

        //String where = "from CohortEntityDG"
        //        + " WHERE id = :id";

        //CohortEntityDG cohortEntity = entityManager.createQuery(where, CohortEntityDG.class)
        //        .setParameter("id", id)
        //        .getSingleResult();
        CohortEntityDG cohortEntity = entityManager.find(CohortEntityDG.class, id);
        System.out.println(cohortEntity);
        System.out.println(cohortEntity.getXmlContent());

        entityManager.close();

        LibraryItem libraryItem = QueryDocumentSerializer.readLibraryItemFromXml(cohortEntity.getXmlContent());

        return libraryItem;
    }
}
