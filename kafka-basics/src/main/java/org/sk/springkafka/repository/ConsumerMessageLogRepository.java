package org.sk.springkafka.repository;


import org.sk.springkafka.entity.ConsumerMessageLog;
import org.springframework.data.jpa.repository.JpaRepository;

public interface ConsumerMessageLogRepository extends JpaRepository<ConsumerMessageLog, Long> {

}
