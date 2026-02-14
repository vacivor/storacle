plugins {
    `java-library`
}

java {
    toolchain { languageVersion = JavaLanguageVersion.of(25) }
}

val minioVersion: String by rootProject.extra

dependencies {
    api(project(":storacle-api"))
    implementation("io.minio:minio:$minioVersion")
}
