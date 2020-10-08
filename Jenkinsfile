#!groovy
milestone 0
node('docker') {
    slackJobDescription = "job '${env.JOB_NAME} [${env.BUILD_NUMBER}]' (${env.BUILD_URL})"
    try {
        stage "Build"
        checkout scm

	repo = sh(returnStdout: true, script: 'grep "(defproject" project.clj | sed -E \'s%.defproject[^/]*/?([^ ]+) .*%\\1%\'').trim()
	echo repo

        descriptive_version = sh(returnStdout: true, script: 'git describe --long --tags --dirty --always').trim()
        echo descriptive_version

        dockerRepo = "test-${env.BUILD_TAG}"

        sh "docker build --rm -t ${dockerRepo} ."

        dockerTestRunner = "test-${env.BUILD_TAG}"
        dockerTestCleanup = "test-cleanup-${env.BUILD_TAG}"
        dockerDeployer = "deploy-${env.BUILD_TAG}"
        try {
            stage "Test"
            try {
                sh "docker run --rm --name ${dockerTestRunner} -v \$(pwd)/test2junit:/usr/src/app/test2junit ${dockerRepo}"
            } finally {
                junit 'test2junit/xml/*.xml'

                sh "docker run --rm --name ${dockerTestCleanup} -v \$(pwd):/build -w /build alpine rm -r test2junit"
            }

            milestone 100
            stage "Deploy"
            lock("lein-deploy-${env.JOB_NAME}") {
              milestone 101
              withCredentials([[$class: 'UsernamePasswordMultiBinding', credentialsId: 'jenkins-clojars-credentials', usernameVariable: 'LEIN_USERNAME', passwordVariable: 'LEIN_PASSWORD']]) {
                  sh "docker run --name ${dockerDeployer} -e LEIN_USERNAME -e LEIN_PASSWORD --rm ${dockerRepo} lein deploy clojars"
              }
            }
        } finally {
            sh returnStatus: true, script: "docker kill ${dockerTestRunner}"
            sh returnStatus: true, script: "docker rm ${dockerTestRunner}"

            sh returnStatus: true, script: "docker kill ${dockerTestCleanup}"
            sh returnStatus: true, script: "docker rm ${dockerTestCleanup}"

            sh returnStatus: true, script: "docker kill ${dockerDeployer}"
            sh returnStatus: true, script: "docker rm ${dockerDeployer}"

            sh returnStatus: true, script: "docker rmi ${dockerRepo}"
        }
    } catch (InterruptedException e) {
        currentBuild.result = "ABORTED"
        slackSend color: 'warning', message: "ABORTED: ${slackJobDescription}"
        throw e
    } catch (e) {
        currentBuild.result = "FAILED"
        sh "echo ${e}"
        slackSend color: 'danger', message: "FAILED: ${slackJobDescription}"
        throw e
    }
}
