package com.example.outbox.worker

import org.slf4j.LoggerFactory
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component

@Component
class WorkerJob {
    private val log = LoggerFactory.getLogger(javaClass)

    @Scheduled(fixedDelayString = "\${worker.poll-interval-ms:5000}")
    fun poll() {
        // Add queue consumption or outbox polling here.
        log.debug("Worker polling tick")
    }
}
