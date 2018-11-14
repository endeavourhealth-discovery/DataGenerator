package org.endeavourhealth.scheduler.models.repository;

import org.endeavourhealth.scheduler.models.database.DatasetEntity;

import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Slice;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.jpa.repository.QueryHints;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import javax.persistence.QueryHint;
import java.util.stream.Stream;

import static org.hibernate.jpa.QueryHints.HINT_FETCH_SIZE;

@Repository
public interface DatasetEntityRepository extends JpaRepository<DatasetEntity, Long> {

    @QueryHints(value = @QueryHint(name = HINT_FETCH_SIZE, value = "" + Integer.MIN_VALUE))
    @Query(value = "select e from DatasetEntity e")
    Stream<DatasetEntity> streamAll();

    @Transactional(readOnly = true)
    Slice<DatasetEntity> findAllBy(Pageable page);
}

