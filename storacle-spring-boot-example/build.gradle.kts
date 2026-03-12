plugins {
    id("org.springframework.boot") version "4.0.2"
    id("io.spring.dependency-management")
}

dependencies {
    implementation(project(":storacle-spring-boot-starter"))
    runtimeOnly(project(":storacle-vendor-minio"))

    implementation("org.springframework.boot:spring-boot-starter-web")
    testImplementation("org.springframework.boot:spring-boot-starter-test")
}
