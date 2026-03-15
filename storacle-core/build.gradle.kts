val tikaVersion: String by rootProject.extra

dependencies {
    api(project(":storacle-api"))
    api("org.apache.tika:tika-core:$tikaVersion")
}
