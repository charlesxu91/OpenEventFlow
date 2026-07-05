plugins {
    kotlin("jvm") version "2.4.0"
    `maven-publish`
}

group = "io.openeventflow"
version = "0.1.0"

kotlin {
    jvmToolchain(17)
}

publishing {
    publications {
        create<MavenPublication>("maven") {
            from(components["java"])
            artifactId = "openeventflow-mobile"
        }
    }
}
