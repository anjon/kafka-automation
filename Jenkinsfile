pipeline {
    agent {
        docker {
            image 'python:3.14-slim'
            // Ensure this connects to your specific lab network
            args '--network kafka-automation_kafka-net'
        }
    }

    environment {
        // Using the internal broker name defined in your Docker Compose
        BOOTSTRAP_SERVERS = 'broker-1:19092,broker-2:19092,broker-3:19092'
        TOPOLOGY_FILE = 'topics_descriptor.yml'
        ACLS_FILE = 'acls_descriptor.yml'
    }
    // Define flags to track stage success
    script {
        def topicsSuccess = false
        def aclsSuccess   = false
    }

    stages {
        stage('Checkout') {
            steps {
                checkout scm
            }
        }

        stage('Install Dependencies') {
            steps {
                sh 'pip install confluent-kafka PyYAML'
            }
        }

        stage('Managing Kafka Topics') {
            steps {
                script {
                    try {
                        sh "python topics_deployment.py --bootstrap ${BOOTSTRAP_SERVERS} --file ${TOPOLOGY_FILE}"
                        topicsSuccess = true
                    } catch (Exception e) {
                        topicsSuccess = false
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
                // Build a dynamic report based on which flags were set to true
                def statusMessage = "--- Kafka Automation Report ---\n"
                statusMessage += "Topics Sync: ${topicsSuccess ? '✅ SUCCESS' : '❌ FAILED'}\n"
                statusMessage += "ACLs Sync:   ${aclsSuccess ? '✅ SUCCESS' : '❌ FAILED'}\n"
                
                echo statusMessage
            }
        }
        
        success {
            echo "All infrastructure and security changes deployed successfully to ${env.BOOTSTRAP_SERVERS}"
        }

        failure {
            echo "Deployment failed. Please check the logs to see if it was a Topic or ACL issue."
        }
    }
}