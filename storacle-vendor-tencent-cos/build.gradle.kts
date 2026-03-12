val tencentCosVersion: String by rootProject.extra

dependencies {
    api(project(":storacle-api"))
    implementation("com.qcloud:cos_api:$tencentCosVersion")
}
