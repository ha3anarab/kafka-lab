package com.hassan.kafka.repository;

import com.hassan.kafka.entity.WikimediaEventEntity;
import org.springframework.data.jpa.repository.JpaRepository;

public interface WikimediaDataRepository extends JpaRepository<WikimediaEventEntity, Long> {

    boolean existsByEventId(String eventId);

    long countByEventId(String eventId);
}
