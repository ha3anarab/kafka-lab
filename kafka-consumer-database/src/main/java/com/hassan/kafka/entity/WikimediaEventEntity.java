package com.hassan.kafka.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.Getter;
import lombok.Setter;

/**
 * Storage-side mirror of the Kafka record. {@code eventId} is the Wikimedia event id and the
 * natural key: the UNIQUE constraint is what actually makes the consumer idempotent.
 */
@Getter
@Setter
@Entity
@Table(name = "wikimedia_event")
public class WikimediaEventEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    Long id;

    @Column(name = "event_id", nullable = false, unique = true, length = 64)
    String eventId;

    String type;
    String title;

    /** H2 treats {@code USER} as a reserved word — the column must be renamed, not quoted. */
    @Column(name = "wiki_user")
    String user;

    String serverName;
    String timestamp;
}
