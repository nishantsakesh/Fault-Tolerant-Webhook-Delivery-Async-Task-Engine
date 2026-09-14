package com.webhook.engine.repository;

import com.webhook.engine.domain.DeliveryStatus;
import com.webhook.engine.domain.WebhookDelivery;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.util.UUID;

@Repository
public interface WebhookDeliveryRepository extends JpaRepository<WebhookDelivery, UUID> {

    @Modifying
    @Query("""
        UPDATE WebhookDelivery w
        SET w.status = :status,
            w.attempts = :attempts,
            w.lastHttpStatus = :httpStatus,
            w.lastErrorReason = :errorReason,
            w.updatedAt = :updatedAt
        WHERE w.id = :id
    """)
    void updateDeliveryState(
        @Param("id") UUID id,
        @Param("status") DeliveryStatus status,
        @Param("attempts") int attempts,
        @Param("lastHttpStatus") Integer httpStatus,
        @Param("errorReason") String errorReason,
        @Param("updatedAt") Instant updatedAt
    );
}
