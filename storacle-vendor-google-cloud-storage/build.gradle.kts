plugins {
    `java-library`
}

java {
    toolchain { languageVersion = JavaLanguageVersion.of(25) }
}

val googleCloudStorageVersion: String by rootProject.extra

dependencies {
    api(project(":storacle-api"))
    implementation("com.google.cloud:google-cloud-storage:$googleCloudStorageVersion")
}
