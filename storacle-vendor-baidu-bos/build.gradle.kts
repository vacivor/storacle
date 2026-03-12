val baiduBosVersion: String by rootProject.extra

dependencies {
    api(project(":storacle-api"))
    implementation("com.baidubce:bce-java-sdk:$baiduBosVersion")
}
