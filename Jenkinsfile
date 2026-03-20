pipeline {
    agent {
        docker {
            image 'python:3.14-slim'
            // Ensure this connects to your specific lab network
            args '--network apache-kafka-cluster_kafka-net'
        }
    }

    environment {
        // Using the internal broker name defined in your Docker Compose
        BOOTSTRAP_SERVERS = 'broker-1:19092,broker-2:19092,broker-3:19092'
        TOPOLOGY_FILE = 'topics_descriptor.yml'
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

        stage('Sync Kafka Topics') {
            steps {
                script {
                    // We pass the bootstrap server as an argument to your script
                    sh "python sync_topics.py --bootstrap ${BOOTSTRAP_SERVERS} --file ${TOPOLOGY_FILE}"
                }
            }
        }
    }

    post {
        success {
            echo "Kafka topics are synchronized with Git!"
        }
        failure {
            echo "Failed to sync topics. Check the logs for Kafka connectivity issues."
        }
    }
}