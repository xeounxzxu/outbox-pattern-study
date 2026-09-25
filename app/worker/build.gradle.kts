dependencies {
    implementation("org.springframework.boot:spring-boot-starter-webmvc")
    testImplementation("org.apache.kafka:kafka-clients")
    testImplementation(project(":app:api"))
    testImplementation("org.testcontainers:testcontainers-kafka")
}
