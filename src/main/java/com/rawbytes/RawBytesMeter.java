package com.rawbytes;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;

public class RawBytesMeter {

    private static final DateTimeFormatter UTC_FMT =
        DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'")
            .withZone(ZoneOffset.UTC);

    public static void main(String[] args) throws Exception {
        if (args.length == 1 && !args[0].startsWith("-")) {
            runMeasure(loadConfig(args[0]));
            return;
        }

        if (args.length != 3 || !args[1].equals("--config")) {
            printUsageAndExit();
        }

        Properties config = loadConfig(args[2]);
        switch (args[0]) {
            case "measure":
                runMeasure(config);
                break;
            case "discover":
                discoverTopics(config).forEach(System.out::println);
                break;
            default:
                printUsageAndExit();
        }
    }

    private static void runMeasure(Properties config) throws Exception {
        String bootstrapServers = require(config, "bootstrap.servers");
        String appId = config.getProperty("application.id", "raw-byte-meter");
        String summaryTopic = require(config, "summary.topic");
        int intervalSeconds = intProp(config, "summary.interval.seconds", 60);
        int logIntervalSeconds = intProp(config, "log.interval.seconds", 30);
        int commitIntervalSeconds = intProp(config, "commit.interval.seconds", 30);
        boolean hourlyEnabled = booleanProp(config, "hourly.enabled", true);
        String offsetReset = config.getProperty("auto.offset.reset", "latest");
        List<String> sourceTopics = resolveSourceTopics(config);

        Properties consumerProps = kafkaProps(config);
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, appId);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
            ByteArrayDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
            ByteArrayDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, offsetReset);

        Properties producerProps = kafkaProps(config);
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
            ByteArraySerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
            ByteArraySerializer.class.getName());
        producerProps.put(ProducerConfig.ACKS_CONFIG, "all");

        CountDownLatch shutdown = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(shutdown::countDown));

        Map<Long, Map<String, ByteMetrics>> intervalBuckets = new HashMap<>();
        Map<Long, Map<String, ByteMetrics>> hourlyBuckets = new HashMap<>();
        Totals totals = new Totals();
        CommitStatus commitStatus = new CommitStatus();
        Instant lastLog = Instant.now();
        long lastCommitMs = 0;
        boolean commitNeeded = false;

        System.err.printf("raw-byte-meter measure starting: group=%s summary.topic=%s%n",
            appId, summaryTopic);
        System.err.printf("topics=%d interval=%ds hourly=%s auto.offset.reset=%s commit.interval=%ds%n",
            sourceTopics.size(), intervalSeconds, hourlyEnabled, offsetReset, commitIntervalSeconds);
        System.err.println("monitoring=" + String.join(",", sourceTopics));

        try (
            KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(consumerProps);
            KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(producerProps)
        ) {
            consumer.subscribe(sourceTopics);

            while (shutdown.getCount() > 0) {
                ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofSeconds(1));
                long nowMs = System.currentTimeMillis();
                long intervalStartMs = floorMs(nowMs, intervalSeconds);
                long hourStartMs = floorMs(nowMs, 3600);

                for (ConsumerRecord<byte[], byte[]> record : records) {
                    ByteMetrics delta = metricsFor(record);
                    addMetric(intervalBuckets, intervalStartMs, record.topic(), delta);
                    if (hourlyEnabled) {
                        addMetric(hourlyBuckets, hourStartMs, record.topic(), delta);
                    }
                    totals.records += delta.recordCount;
                    totals.totalBytes += delta.totalBytes;
                }

                if (!records.isEmpty()) {
                    commitNeeded = true;
                }

                if (commitNeeded &&
                    nowMs - lastCommitMs >= Duration.ofSeconds(commitIntervalSeconds).toMillis()) {
                    consumer.commitAsync((offsets, ex) ->
                        commitStatus.recordAsyncResult(ex, System.currentTimeMillis()));
                    lastCommitMs = nowMs;
                    commitNeeded = false;
                }

                emitReadyWindows(producer, summaryTopic, "interval",
                    intervalBuckets, intervalStartMs, intervalSeconds);
                if (hourlyEnabled) {
                    emitReadyWindows(producer, summaryTopic, "hourly",
                        hourlyBuckets, hourStartMs, 3600);
                }

                Instant now = Instant.ofEpochMilli(nowMs);
                if (Duration.between(lastLog, now).getSeconds() >= logIntervalSeconds) {
                    logStatus(consumer, totals, intervalBuckets, hourlyBuckets);
                    lastLog = now;
                }
            }

            System.err.println("shutdown requested; flushing open windows");
            emitAllWindows(producer, summaryTopic, "interval", intervalBuckets, intervalSeconds);
            if (hourlyEnabled) {
                emitAllWindows(producer, summaryTopic, "hourly", hourlyBuckets, 3600);
            }
            producer.flush();
            consumer.commitSync();
        }
    }

    private static List<String> resolveSourceTopics(Properties config) throws Exception {
        String mode = config.getProperty("topics.mode", "list").trim();
        if (mode.equalsIgnoreCase("discover")) {
            return discoverTopics(config);
        }

        String raw = require(config, "source.topics");
        List<String> topics = splitCsv(raw);
        if (topics.isEmpty()) {
            throw new IllegalArgumentException("source.topics parsed to an empty list");
        }
        return topics;
    }

    private static List<String> discoverTopics(Properties config) throws Exception {
        String bootstrapServers = require(config, "bootstrap.servers");
        List<String> excludes = splitCsv(config.getProperty("exclude.topics", ""));
        excludes.add(config.getProperty("application.id", "raw-byte-meter"));
        excludes.add(config.getProperty("summary.topic", ""));

        Properties adminProps = kafkaProps(config);
        adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        try (AdminClient admin = AdminClient.create(adminProps)) {
            Collection<String> names = admin.listTopics(new ListTopicsOptions()
                    .listInternal(false))
                .names()
                .get();
            return names.stream()
                .filter(name -> !name.startsWith("_"))
                .filter(name -> !isExcluded(name, excludes))
                .sorted()
                .collect(Collectors.toList());
        }
    }

    private static void emitReadyWindows(
        KafkaProducer<byte[], byte[]> producer,
        String summaryTopic,
        String windowType,
        Map<Long, Map<String, ByteMetrics>> buckets,
        long currentWindowStartMs,
        int windowSeconds
    ) {
        List<Long> ready = buckets.keySet().stream()
            .filter(windowStartMs -> windowStartMs < currentWindowStartMs)
            .sorted()
            .collect(Collectors.toList());

        for (Long windowStartMs : ready) {
            emitWindow(producer, summaryTopic, windowType, windowStartMs,
                windowSeconds, buckets.remove(windowStartMs));
        }
    }

    private static void emitAllWindows(
        KafkaProducer<byte[], byte[]> producer,
        String summaryTopic,
        String windowType,
        Map<Long, Map<String, ByteMetrics>> buckets,
        int windowSeconds
    ) {
        List<Long> starts = new ArrayList<>(buckets.keySet());
        starts.sort(Long::compareTo);
        for (Long windowStartMs : starts) {
            emitWindow(producer, summaryTopic, windowType, windowStartMs,
                windowSeconds, buckets.remove(windowStartMs));
        }
    }

    private static void emitWindow(
        KafkaProducer<byte[], byte[]> producer,
        String summaryTopic,
        String windowType,
        long windowStartMs,
        int windowSeconds,
        Map<String, ByteMetrics> perTopic
    ) {
        if (perTopic == null || perTopic.isEmpty()) return;

        long windowEndMs = windowStartMs + Duration.ofSeconds(windowSeconds).toMillis();
        System.err.printf("emitting %s window %s -> %s topics=%d%n",
            windowType, fmt(windowStartMs), fmt(windowEndMs), perTopic.size());

        for (Map.Entry<String, ByteMetrics> entry : new TreeMap<>(perTopic).entrySet()) {
            String topic = entry.getKey();
            ByteMetrics metrics = entry.getValue();
            String json = summaryJson(windowType, windowStartMs, windowEndMs, topic, metrics);
            byte[] key = String.format("%s:%s:%s", windowType, fmt(windowStartMs), topic)
                .getBytes(StandardCharsets.UTF_8);
            byte[] value = json.getBytes(StandardCharsets.UTF_8);
            producer.send(new ProducerRecord<>(summaryTopic, key, value), (metadata, ex) -> {
                if (ex != null) {
                    System.err.printf("ERROR produce summary topic=%s window=%s source=%s: %s%n",
                        summaryTopic, fmt(windowStartMs), topic, ex.getMessage());
                } else {
                    System.err.printf("summary produced type=%s source=%s bytes=%d records=%d partition=%d offset=%d%n",
                        windowType, topic, metrics.totalBytes, metrics.recordCount,
                        metadata.partition(), metadata.offset());
                }
            });
        }
        producer.flush();
    }

    private static String summaryJson(
        String windowType,
        long windowStartMs,
        long windowEndMs,
        String topic,
        ByteMetrics metrics
    ) {
        return "{"
            + "\"window_type\":\"" + jsonEscape(windowType) + "\","
            + "\"window_start\":\"" + fmt(windowStartMs) + "\","
            + "\"window_end\":\"" + fmt(windowEndMs) + "\","
            + "\"topic\":\"" + jsonEscape(topic) + "\","
            + "\"record_count\":" + metrics.recordCount + ","
            + "\"key_bytes\":" + metrics.keyBytes + ","
            + "\"value_bytes\":" + metrics.valueBytes + ","
            + "\"header_bytes\":" + metrics.headerBytes + ","
            + "\"total_bytes\":" + metrics.totalBytes
            + "}";
    }

    private static ByteMetrics metricsFor(ConsumerRecord<byte[], byte[]> record) {
        long keyBytes = record.key() == null ? 0 : record.key().length;
        long valueBytes = record.value() == null ? 0 : record.value().length;
        long headerBytes = headerSize(record.headers());
        return new ByteMetrics(keyBytes, valueBytes, headerBytes, 1);
    }

    private static void addMetric(
        Map<Long, Map<String, ByteMetrics>> buckets,
        long windowStartMs,
        String topic,
        ByteMetrics delta
    ) {
        ByteMetrics metrics = buckets
            .computeIfAbsent(windowStartMs, ignored -> new HashMap<>())
            .computeIfAbsent(topic, ignored -> new ByteMetrics());
        metrics.add(delta);
    }

    private static void logStatus(
        KafkaConsumer<byte[], byte[]> consumer,
        Totals totals,
        Map<Long, Map<String, ByteMetrics>> intervalBuckets,
        Map<Long, Map<String, ByteMetrics>> hourlyBuckets
    ) {
        System.err.printf("status records=%d total_bytes=%d open_interval_windows=%d open_hourly_windows=%d assignments=%d%n",
            totals.records, totals.totalBytes, intervalBuckets.size(),
            hourlyBuckets.size(), consumer.assignment().size());

        Set<TopicPartition> assignment = consumer.assignment();
        if (assignment.isEmpty()) return;

        try {
            Map<TopicPartition, Long> ends = consumer.endOffsets(assignment);
            long totalLag = 0;
            for (TopicPartition tp : assignment) {
                long position = consumer.position(tp);
                totalLag += Math.max(0, ends.getOrDefault(tp, position) - position);
            }
            System.err.printf("status consumer_lag_records=%d%n", totalLag);
        } catch (RuntimeException ex) {
            System.err.printf("WARN unable to compute lag: %s%n", ex.getMessage());
        }
    }

    static long headerSize(Headers headers) {
        if (headers == null) return 0;
        long size = 0;
        for (Header h : headers) {
            if (h.key() != null) size += h.key().getBytes(StandardCharsets.UTF_8).length;
            if (h.value() != null) size += h.value().length;
        }
        return size;
    }

    private static Properties loadConfig(String path) throws IOException {
        Properties props = new Properties();
        try (FileInputStream fis = new FileInputStream(path)) {
            props.load(fis);
        }
        return props;
    }

    private static Properties kafkaProps(Properties config) {
        Properties out = new Properties();
        for (String key : new String[]{
            "security.protocol",
            "sasl.mechanism",
            "sasl.jaas.config",
            "ssl.truststore.location", "ssl.truststore.password",
            "ssl.keystore.location", "ssl.keystore.password",
            "ssl.key.password",
            "ssl.endpoint.identification.algorithm"
        }) {
            String value = config.getProperty(key);
            if (value != null) out.put(key, value);
        }
        return out;
    }

    private static List<String> splitCsv(String raw) {
        if (raw == null || raw.isBlank()) return List.of();
        return Arrays.stream(raw.split(","))
            .map(String::trim)
            .filter(s -> !s.isEmpty())
            .collect(Collectors.toList());
    }

    private static boolean isExcluded(String topic, List<String> excludes) {
        for (String exclude : excludes) {
            if (exclude.endsWith("*")) {
                String prefix = exclude.substring(0, exclude.length() - 1);
                if (topic.startsWith(prefix)) return true;
            } else if (topic.equals(exclude)) {
                return true;
            }
        }
        return false;
    }

    private static String require(Properties props, String key) {
        String value = props.getProperty(key);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("required property missing: " + key);
        }
        return value.trim();
    }

    private static int intProp(Properties props, String key, int fallback) {
        return Integer.parseInt(props.getProperty(key, String.valueOf(fallback)).trim());
    }

    private static boolean booleanProp(Properties props, String key, boolean fallback) {
        return Boolean.parseBoolean(props.getProperty(key, String.valueOf(fallback)).trim());
    }

    private static long floorMs(long epochMs, int windowSeconds) {
        long windowMs = Duration.ofSeconds(windowSeconds).toMillis();
        return (epochMs / windowMs) * windowMs;
    }

    private static String fmt(long epochMs) {
        return UTC_FMT.format(Instant.ofEpochMilli(epochMs));
    }

    private static String jsonEscape(String s) {
        return s.replace("\\", "\\\\")
            .replace("\"", "\\\"")
            .replace("\n", "\\n")
            .replace("\r", "\\r")
            .replace("\t", "\\t");
    }

    private static void printUsageAndExit() {
        System.err.println("Usage:");
        System.err.println("  java -jar raw-byte-meter.jar measure --config config.properties");
        System.err.println("  java -jar raw-byte-meter.jar discover --config config.properties");
        System.err.println("  java -jar raw-byte-meter.jar config.properties  # shorthand for measure");
        System.exit(1);
    }

    private static final class Totals {
        long records;
        long totalBytes;
    }

    private static final class CommitStatus {
        private long consecutiveFailures;
        private long lastWarningMs;

        void recordAsyncResult(Exception ex, long nowMs) {
            if (ex == null) {
                if (consecutiveFailures > 0) {
                    System.err.printf("offset commit recovered after %d failure(s)%n",
                        consecutiveFailures);
                }
                consecutiveFailures = 0;
                return;
            }

            consecutiveFailures++;
            if (nowMs - lastWarningMs >= Duration.ofMinutes(1).toMillis()) {
                System.err.printf("WARN offset commit failed %d consecutive time(s); will retry: %s%n",
                    consecutiveFailures, ex.getMessage());
                lastWarningMs = nowMs;
            }
        }
    }
}
