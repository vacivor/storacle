val qiniuVersion: String by rootProject.extra

dependencies {
    api(project(":storacle-api"))
    implementation("com.qiniu:qiniu-java-sdk:$qiniuVersion")
}
