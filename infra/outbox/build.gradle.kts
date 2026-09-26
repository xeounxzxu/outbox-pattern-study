plugins { `java-library` }

dependencies {
    api("one.tomorrow.transactional-outbox:outbox-kafka-spring:4.0.1-SNAPSHOT") {
        exclude(group = "org.postgresql", module = "postgresql")
    }
    api("org.springframework.boot:spring-boot-starter-jdbc")
    implementation("org.springframework.boot:spring-boot-starter-flyway")
    runtimeOnly("org.flywaydb:flyway-mysql")
    runtimeOnly("com.mysql:mysql-connector-j")
}
