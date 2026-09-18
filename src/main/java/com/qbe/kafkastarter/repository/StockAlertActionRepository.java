package com.qbe.kafkastarter.repository;

import com.qbe.kafkastarter.entity.StockAlertActionEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface StockAlertActionRepository extends JpaRepository<StockAlertActionEntity, Long> {

    boolean existsByEventId(String eventId);
}
