plugins {
    `java-library`
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

dependencies {
    api(project(":storacle-api"))
    testImplementation("org.junit.jupiter:junit-jupiter:5.11.0")
}
