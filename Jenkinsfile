// Global variables must be defined here to be shared across stages
def topicsSuccess = false
def aclsSuccess   = false

pipeline {
    agent {
        docker {
            image 'python:3.14-slim'
            // Using the network name we defined in your Docker Compose
            args '--network kafka-automation_kafka-net'
        }
    }

    environment {
        BOOTSTRAP_SERVERS = 'broker-1:19092,broker-2:19092,broker-3:19092'
        TOPOLOGY_FILE     = 'topics_descriptor.yml'
        ACLS_FILE         = 'acls_descriptor.yml'
    }

    stages {
        stage('Checkout') {
            steps {
                checkout scm
            }
        }

        stage('Install Dependencies') {
            steps {
                // Ensure the agent has the required libraries for the scripts
                sh '''
                    python --version
                    pip install --no-cache-dir confluent-kafka PyYAML
                '''
            }
        }

        stage('Managing Kafka Topics') {
            steps {
                script {
                    try {
                        sh "python topics_manager.py --bootstrap ${BOOTSTRAP_SERVERS} --file ${TOPOLOGY_FILE}"
                        topicsSuccess = true
                    } catch (Exception e) {
                        topicsSuccess = false
                        // This stops the pipeline and marks it as FAILED
                        error "Failed to sync Kafka topics: ${e.message}"
                    }
                }
            }
        }

        stage('Managing Kafka ACLs') {
            steps {
                script {
                    try {
                        sh "python acls_manager.py --bootstrap ${BOOTSTRAP_SERVERS} --file ${ACLS_FILE}"
                        aclsSuccess = true
                    } catch (Exception e) {
                        aclsSuccess = false
                        error "Failed to sync Kafka ACLs: ${e.message}"
                    }
                }
            }
        }
    }

    post {
        always {
            script {
                // This block runs regardless of success or failure
                def statusReport = """
------------------------------------
   KAFKA AUTOMATION REPORT
------------------------------------
Topics Sync: ${topicsSuccess ? '✅ SUCCESS' : '❌ FAILED'}
ACLs Sync:   ${aclsSuccess ? '✅ SUCCESS' : '❌ FAILED'}
------------------------------------
"""
                echo statusReport
            }
        }
        
        success {
            echo "Successfully deployed all changes to ${env.BOOTSTRAP_SERVERS}"
        }

        failure {
            echo "Deployment failed. Check the report above to identify the failing stage."
        }
    }
}