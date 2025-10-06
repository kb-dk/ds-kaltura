
pipeline {
    agent {
        label 'DS agent'
    }

    options {
        disableConcurrentBuilds()
        timeout(time: 25, unit: 'MINUTES')
        buildDiscarder(logRotator(numToKeepStr: '5'))
    }

    environment {
        MVN_SETTINGS = '/etc/m2/settings.xml' //This should be changed in Jenkins config for the DS agent
        PROJECT = 'ds-kaltura'
        BUILD_TO_TRIGGER = 'ds-datahandler'
    }

    triggers {
        // This triggers the pipeline when a PR is opened or updated or so I hope
        githubPush()
    }

    parameters {
        string(name: 'ORIGINAL_BRANCH', defaultValue: "${env.BRANCH_NAME}", description: 'Branch of first job to run, will also be PI_ID for a PR')
        string(name: 'ORIGINAL_JOB', defaultValue: "ds-kaltura", description: 'What job was the first to build?')
        string(name: 'TARGET_BRANCH', defaultValue: "${env.CHANGE_TARGET}", description: 'Target branch if PR')
        string(name: 'SOURCE_BRANCH', defaultValue: "${env.CHANGE_BRANCH}", description: 'Source branch if PR')
    }

    stages {
        stage('Echo Environment Variables') {
            steps {
                echo "PROJECT: ${env.PROJECT}"
                echo "BUILD_TO_TRIGGER: ${env.BUILD_TO_TRIGGER}"
                echo "ORIGINAL_BRANCH: ${params.ORIGINAL_BRANCH}"
                echo "ORIGINAL_JOB: ${params.ORIGINAL_JOB}"
                echo "TARGET_BRANCH: ${params.TARGET_BRANCH}"
                echo "SOURCE_BRANCH: ${params.SOURCE_BRANCH}"
            }
        }

        stage('Change version if part of PR') {
            when {
                expression {
                    params.ORIGINAL_BRANCH ==~ "PR-[0-9]+"
                }
            }
            steps {
                script {
                    sh "mvn -s ${env.MVN_SETTINGS} versions:set -DnewVersion=${params.ORIGINAL_BRANCH}-${params.ORIGINAL_JOB}-${env.PROJECT}-SNAPSHOT"
                    echo "Changing MVN version to: ${params.ORIGINAL_BRANCH}-${params.ORIGINAL_JOB}-${env.PROJECT}-SNAPSHOT"
                }
            }
        }

        stage('Change dependencies') {
            when {
                expression {
                    params.ORIGINAL_BRANCH ==~ "PR-[0-9]+"
                }
            }
            steps {
                script {
                    switch (params.ORIGINAL_JOB) {
                        case ['ds-parent']:
                            sh "mvn -s ${env.MVN_SETTINGS} versions:use-dep-version -Dincludes=dk.kb.dsparent:* -DdepVersion=${params.ORIGINAL_BRANCH}-${params.ORIGINAL_JOB}-ds-parent-SNAPSHOT -DforceVersion=true"
                            sh "mvn -s ${env.MVN_SETTINGS} versions:use-dep-version -Dincludes=dk.kb.dsshared:* -DdepVersion=${params.ORIGINAL_BRANCH}-${params.ORIGINAL_JOB}-ds-shared-SNAPSHOT -DforceVersion=true"

                            echo "Changing MVN dependency ds-parent to: ${params.ORIGINAL_BRANCH}-${params.ORIGINAL_JOB}-ds-parent-SNAPSHOT"
                            echo "Changing MVN dependency ds-shared to: ${params.ORIGINAL_BRANCH}-${params.ORIGINAL_JOB}-ds-shared-SNAPSHOT"
                            break
                        case ['ds-shared']:
                            sh "mvn -s ${env.MVN_SETTINGS} versions:use-dep-version -Dincludes=dk.kb.dsshared:* -DdepVersion=${params.ORIGINAL_BRANCH}-${params.ORIGINAL_JOB}-ds-shared-SNAPSHOT -DforceVersion=true"

                            echo "Changing MVN dependency ds-shared to: ${params.ORIGINAL_BRANCH}-${params.ORIGINAL_JOB}-ds-shared-SNAPSHOT"
                            break
                    }
                }
            }
        }

        stage('Build') {
            steps {
                withMaven(options: [artifactsPublisher(fingerprintFilesDisabled: true, archiveFilesDisabled: true)], traceability: true) {
                    // Execute Maven build
                    sh "mvn -s ${env.MVN_SETTINGS} clean package"
                }
            }
        }

        stage('Analyze build results') {
            steps {
                recordIssues(
                    aggregatingResults: true,
                    tools: [
                        java(),
                        javaDoc(),
                        mavenConsole(),
                        taskScanner(
                            highTags: 'FIXME',
                            normalTags: 'TODO',
                            includePattern: '**/*.java',
                            excludePattern: 'target/**/*'
                        )
                    ]
                )
            }
        }

        stage('Push to Nexus') {
            when {
                // Check if Build was successful
                expression {
                    currentBuild.currentResult == "SUCCESS" && params.ORIGINAL_BRANCH ==~ "master|release_v[0-9]+|PR-[0-9]+"
                }
            }
            steps {
                withMaven(options: [artifactsPublisher(fingerprintFilesDisabled: true, archiveFilesDisabled: true)], traceability: true) {
                    sh "mvn -s ${env.MVN_SETTINGS} clean deploy -DskipTests=true"
                }
            }
        }

        stage('Trigger ds-datahandler Build') {
            when {
                expression {
                    currentBuild.currentResult == "SUCCESS" && params.ORIGINAL_BRANCH ==~ "master|release_v[0-9]+|PR-[0-9]+"
                }
            }
            steps {
                script {
                    if (params.ORIGINAL_BRANCH ==~ "PR-[0-9]+") {
                        // Check the next job to trigger, if there is a branch with same name as the source branch (if a task has involved multiple repositories)
                        def EMPTY_IF_NO_BRANCH = sh(script: "git ls-remote --heads https://github.com/kb-dk/${env.BUILD_TO_TRIGGER}.git | grep 'refs/heads/${params.SOURCE_BRANCH}' || echo empty", returnStdout: true).trim()
                        echo "EMPTY_IF_NO_BRANCH: ${EMPTY_IF_NO_BRANCH}"

                        if ("${EMPTY_IF_NO_BRANCH}" == "empty") {
                            BRANCH_TO_USE = "${params.TARGET_BRANCH}"
                        } else {
                            BRANCH_TO_USE = "${params.SOURCE_BRANCH}"
                        }

                        echo "Triggering: DS-GitHub/${env.BUILD_TO_TRIGGER}/${BRANCH_TO_USE}"

                        build job: "DS-GitHub/${env.BUILD_TO_TRIGGER}/${BRANCH_TO_USE}",
                            parameters: [
                                string(name: 'ORIGINAL_BRANCH', value: params.ORIGINAL_BRANCH),
                                string(name: 'ORIGINAL_JOB', value: params.ORIGINAL_JOB),
                                string(name: 'TARGET_BRANCH', value: params.TARGET_BRANCH),
                                string(name: 'SOURCE_BRANCH', value: params.SOURCE_BRANCH)
                            ],
                            wait: true // Wait for the pipeline to finish
                    } else if (params.ORIGINAL_BRANCH ==~ "master|release_v[0-9]+") {
                        echo "Triggering: DS-GitHub/${env.BUILD_TO_TRIGGER}/${params.ORIGINAL_BRANCH}"

                        build job: "DS-GitHub/${env.BUILD_TO_TRIGGER}/${params.ORIGINAL_BRANCH}",
                            wait: false // Don't wait for the pipeline to finish
                    }
                }
            }
        }
    }
}
