plugins {
    `java-library`
}

java {
    toolchain { languageVersion = JavaLanguageVersion.of(25) }
}

val awsSdkVersion: String by rootProject.extra

dependencies {
    api(project(":storacle-api"))
    implementation(platform("software.amazon.awssdk:bom:$awsSdkVersion"))
    implementation("software.amazon.awssdk:s3")
    implementation("software.amazon.awssdk:auth")
    implementation("software.amazon.awssdk:regions")
}
