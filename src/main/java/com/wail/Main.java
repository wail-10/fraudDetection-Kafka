package com.wail;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
import io.prometheus.client.exporter.HTTPServer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class Main {
    private static final Counter suspiciousTransactionsTotal = Counter.build()
            .name("suspicious_transactions_total")
            .help("Total number of suspicious transactions detected")
            .labelNames("user_id")
            .register();

    private static final Gauge suspiciousTransactionsAmount = Gauge.build()
            .name("suspicious_transactions_amount")
            .help("Total amount of suspicious transactions")
            .labelNames("user_id")
            .register();

    public static void main(String[] args) throws IOException {
        // Démarrer le serveur HTTP pour exposer les métriques Prometheus
        HTTPServer prometheusServer = new HTTPServer(8080);

        // Configuration Kafka Streams
        Properties properties = getKafkaProperties();
        StreamsBuilder builder = new StreamsBuilder();

        // Création du stream d'entrée
        KStream<String, String> transactions = builder.stream("transactions-input");

        // Traitement des transactions
        KStream<String, String> suspiciousTransactions = transactions.filter((key, value) -> {
            try {
                ObjectMapper mapper = new ObjectMapper();
                JsonNode transaction = mapper.readTree(value);
                double amount = transaction.get("amount").asDouble();

                if (amount > 10000) {
                    // Mise à jour des métriques Prometheus
                    String userId = transaction.get("userId").asText();
                    suspiciousTransactionsTotal.labels(userId).inc();
                    suspiciousTransactionsAmount.labels(userId).set(amount);
                    return true;
                }
                return false;
            } catch (Exception e) {
                e.printStackTrace();
                return false;
            }
        });

        // Envoi vers le topic fraud-alerts
        suspiciousTransactions.to("fraud-alerts", Produced.with(Serdes.String(), Serdes.String()));

        // Démarrage de l'application Kafka Streams
        KafkaStreams streams = new KafkaStreams(builder.build(), properties);
        streams.start();

        // Ajout du hook d'arrêt
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            streams.close();
            prometheusServer.stop();
        }));
    }

    private static Properties getKafkaProperties() {
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "fraud-detection-app");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        return properties;
    }
}