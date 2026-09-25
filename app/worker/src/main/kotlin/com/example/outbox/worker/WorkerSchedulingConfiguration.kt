package com.example.outbox.worker

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.context.annotation.Configuration
import org.springframework.scheduling.annotation.EnableScheduling

@Configuration(proxyBeanMethods = false)
@EnableScheduling
@ConditionalOnProperty(prefix = "worker.scheduling", name = ["enabled"], havingValue = "true", matchIfMissing = true)
class WorkerSchedulingConfiguration
