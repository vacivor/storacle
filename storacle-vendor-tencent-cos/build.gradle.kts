plugins {
    `java-library`
}

java {
    toolchain { languageVersion = JavaLanguageVersion.of(25) }
}

val tencentCosVersion: String by rootProject.extra

dependencies {
    api(project(":storacle-api"))
    implementation("com.qcloud:cos_api:$tencentCosVersion")
}
