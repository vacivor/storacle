plugins {
    `java-library`
}

java {
    toolchain { languageVersion = JavaLanguageVersion.of(25) }
}

val qiniuVersion: String by rootProject.extra

dependencies {
    api(project(":storacle-api"))
    implementation("com.qiniu:qiniu-java-sdk:$qiniuVersion")
}
