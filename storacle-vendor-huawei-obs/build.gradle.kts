val huaweiObsVersion: String by rootProject.extra

dependencies {
    api(project(":storacle-api"))
    implementation("com.huaweicloud:esdk-obs-java:$huaweiObsVersion")
}
