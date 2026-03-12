val minioVersion: String by rootProject.extra

dependencies {
    api(project(":storacle-api"))
    implementation("io.minio:minio:$minioVersion")
}
