package org.sk.springkafka.repository;


import org.sk.springkafka.entity.ProducerMessageLog;
import org.springframework.data.jpa.repository.JpaRepository;

public interface ProducerMessageLogRepository extends JpaRepository<ProducerMessageLog, Long> {

}
