plugins {
    `java-library`
}

java {
    toolchain { languageVersion = JavaLanguageVersion.of(25) }
}

val aliyunOssVersion: String by rootProject.extra

dependencies {
    api(project(":storacle-api"))
    implementation("com.aliyun.oss:aliyun-sdk-oss:$aliyunOssVersion")
}
