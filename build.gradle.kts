import io.spring.gradle.dependencymanagement.dsl.DependencyManagementExtension
import org.jetbrains.kotlin.gradle.dsl.KotlinJvmProjectExtension
import org.springframework.boot.gradle.plugin.SpringBootPlugin

plugins {
    kotlin("jvm") version "2.3.21" apply false
    kotlin("plugin.spring") version "2.3.21" apply false
    id("org.springframework.boot") version "4.1.1" apply false
    id("io.spring.dependency-management") version "1.1.7" apply false
}

configure(listOf(project(":app:api"), project(":app:worker"), project(":infra:outbox"))) {
    apply(plugin = "org.jetbrains.kotlin.jvm")
    apply(plugin = "org.jetbrains.kotlin.plugin.spring")
    apply(plugin = "io.spring.dependency-management")

    group = "com.example.outbox"
    version = "0.0.1-SNAPSHOT"
    repositories { mavenCentral() }

    extensions.configure<DependencyManagementExtension> {
        imports { mavenBom(SpringBootPlugin.BOM_COORDINATES) }
    }
    extensions.configure<KotlinJvmProjectExtension> {
        jvmToolchain(25)
        compilerOptions {
            freeCompilerArgs.addAll("-Xjsr305=strict", "-Xannotation-default-target=param-property")
        }
    }
    dependencies {
        "implementation"("org.jetbrains.kotlin:kotlin-reflect")
        "testImplementation"("org.springframework.boot:spring-boot-starter-test")
        "testImplementation"("org.jetbrains.kotlin:kotlin-test-junit5")
        "testRuntimeOnly"("org.junit.platform:junit-platform-launcher")
    }
    tasks.withType<Test>().configureEach { useJUnitPlatform() }
}

configure(listOf(project(":app:api"), project(":app:worker"))) {
    apply(plugin = "org.springframework.boot")
    dependencies {
        "implementation"(project(":infra:outbox"))
        "implementation"("org.springframework.boot:spring-boot-starter-actuator")
        "testImplementation"("org.testcontainers:testcontainers-postgresql")
    }
}
