plugins {
    `java-library`
    id("io.spring.dependency-management")
}

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(25)
    }
    withSourcesJar()
    withJavadocJar()
}

tasks.withType<Test> {
    useJUnitPlatform()
}

val springBootVersion: String by rootProject.extra

dependencyManagement {
    imports {
        mavenBom("org.springframework.boot:spring-boot-dependencies:$springBootVersion")
    }
}

dependencies {
    api(project(":storacle-core"))
    api("org.springframework.boot:spring-boot-autoconfigure")

    annotationProcessor("org.springframework.boot:spring-boot-configuration-processor")
    compileOnly("org.springframework.boot:spring-boot-configuration-processor")

    testImplementation("org.springframework.boot:spring-boot-starter-test")
}
