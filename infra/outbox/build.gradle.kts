plugins { `java-library` }

dependencies {
    api("one.tomorrow.transactional-outbox:outbox-kafka-spring:4.0.0")
    api("org.springframework.boot:spring-boot-starter-jdbc")
    implementation("org.springframework.boot:spring-boot-starter-flyway")
    runtimeOnly("org.flywaydb:flyway-database-postgresql")
    runtimeOnly("org.postgresql:postgresql")
}
