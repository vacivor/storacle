import org.gradle.api.plugins.JavaPluginExtension

plugins {
    id("io.spring.dependency-management") version "1.1.7" apply false
}

group = "io.vacivor"
version = "0.0.1-SNAPSHOT"
description = "storacle"

val javaVersion = 25
val springBootVersion = "4.0.2"
val awsSdkVersion = "2.25.66"
val minioVersion = "8.6.0"
val tencentCosVersion = "5.6.261"
val qiniuVersion = "7.19.0"
val huaweiObsVersion = "3.25.10"
val googleCloudStorageVersion = "2.62.1"
val baiduBosVersion = "0.10.412"
val aliyunOssVersion = "3.18.5"

extra["springBootVersion"] = springBootVersion
extra["awsSdkVersion"] = awsSdkVersion
extra["minioVersion"] = minioVersion
extra["tencentCosVersion"] = tencentCosVersion
extra["qiniuVersion"] = qiniuVersion
extra["huaweiObsVersion"] = huaweiObsVersion
extra["googleCloudStorageVersion"] = googleCloudStorageVersion
extra["baiduBosVersion"] = baiduBosVersion
extra["aliyunOssVersion"] = aliyunOssVersion

allprojects {
    group = rootProject.group
    version = rootProject.version

    repositories {
        mavenCentral()
    }
}

subprojects {
    apply(plugin = "java-library")

    extensions.configure<JavaPluginExtension> {
        toolchain {
            languageVersion = JavaLanguageVersion.of(javaVersion)
        }
        withSourcesJar()
        withJavadocJar()
    }

    tasks.withType<Test>().configureEach {
        useJUnitPlatform()
    }

    dependencies {
        "testImplementation"("org.junit.jupiter:junit-jupiter:5.11.0")
        "testRuntimeOnly"("org.junit.platform:junit-platform-launcher")
    }
}
