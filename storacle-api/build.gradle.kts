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
