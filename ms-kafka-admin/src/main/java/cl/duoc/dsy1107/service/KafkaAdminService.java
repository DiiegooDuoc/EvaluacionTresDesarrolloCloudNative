package cl.duoc.dsy1107.service;

import cl.duoc.dsy1107.dto.CreateKafkaTopicRequestDTO;
import cl.duoc.dsy1107.dto.UpdateKafkaTopicConfigDTO;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Slf4j
@Service
public class KafkaAdminService {

    private final AdminClient adminClient;

    public KafkaAdminService(AdminClient adminClient) {
        this.adminClient = adminClient;
    }

    public Map<String, Object> obtenerEstadoClusterYBrokers() {
        try {
            DescribeClusterResult cluster = adminClient.describeCluster();
            String clusterId = cluster.clusterId().get(6, TimeUnit.SECONDS);
            Node controller = cluster.controller().get(6, TimeUnit.SECONDS);
            Collection<Node> nodes = cluster.nodes().get(6, TimeUnit.SECONDS);

            List<Map<String, Object>> brokers = nodes.stream().map(n -> {
                Map<String, Object> b = new LinkedHashMap<>();
                b.put("id", n.id());
                b.put("host", n.host());
                b.put("port", n.port());
                b.put("isController", controller != null && controller.id() == n.id());
                return b;
            }).toList();

            Map<String, Object> result = new LinkedHashMap<>();
            result.put("clusterId", clusterId);
            result.put("brokerCount", brokers.size());
            result.put("controllerId", controller != null ? controller.id() : -1);
            result.put("brokers", brokers);
            return result;
        } catch (Exception e) {
            throw new IllegalStateException("No fue posible consultar el clúster Kafka: " + e.getMessage());
        }
    }

    public List<Map<String, Object>> listarTopicosConDetalle() {
        try {
            Set<String> names = adminClient.listTopics().names().get(6, TimeUnit.SECONDS)
                    .stream()
                    .filter(name -> !name.startsWith("__"))
                    .sorted()
                    .collect(Collectors.toCollection(java.util.LinkedHashSet::new));

            if (names.isEmpty()) {
                return Collections.emptyList();
            }

            Map<String, TopicDescription> descriptions =
                    adminClient.describeTopics(names).allTopicNames().get(6, TimeUnit.SECONDS);

            List<ConfigResource> configResources = names.stream()
                    .map(n -> new ConfigResource(ConfigResource.Type.TOPIC, n))
                    .toList();
            Map<ConfigResource, Config> configsMap =
                    adminClient.describeConfigs(configResources).all().get(6, TimeUnit.SECONDS);

            Map<TopicPartition, OffsetSpec> latestSpecs = new HashMap<>();
            Map<TopicPartition, OffsetSpec> earliestSpecs = new HashMap<>();
            for (TopicDescription td : descriptions.values()) {
                for (TopicPartitionInfo pInfo : td.partitions()) {
                    TopicPartition tp = new TopicPartition(td.name(), pInfo.partition());
                    latestSpecs.put(tp, OffsetSpec.latest());
                    earliestSpecs.put(tp, OffsetSpec.earliest());
                }
            }

            Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> endOffsets =
                    adminClient.listOffsets(latestSpecs).all().get(6, TimeUnit.SECONDS);
            Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> startOffsets =
                    adminClient.listOffsets(earliestSpecs).all().get(6, TimeUnit.SECONDS);

            List<Map<String, Object>> response = new ArrayList<>();
            for (String name : names) {
                TopicDescription td = descriptions.get(name);
                Config cfg = configsMap.get(new ConfigResource(ConfigResource.Type.TOPIC, name));

                int partitionCount = td.partitions().size();
                int replicationFactor = partitionCount > 0 ? td.partitions().get(0).replicas().size() : 0;

                long totalMessages = 0;
                List<Map<String, Object>> partitionDetails = new ArrayList<>();
                for (TopicPartitionInfo p : td.partitions()) {
                    TopicPartition tp = new TopicPartition(name, p.partition());
                    long end = endOffsets.containsKey(tp) ? endOffsets.get(tp).offset() : 0L;
                    long start = startOffsets.containsKey(tp) ? startOffsets.get(tp).offset() : 0L;
                    long count = Math.max(0L, end - start);
                    totalMessages += count;

                    Map<String, Object> pMap = new LinkedHashMap<>();
                    pMap.put("partition", p.partition());
                    pMap.put("leader", p.leader() != null ? p.leader().id() : -1);
                    pMap.put("replicas", p.replicas().stream().map(Node::id).toList());
                    pMap.put("isr", p.isr().stream().map(Node::id).toList());
                    pMap.put("endOffset", end);
                    pMap.put("messages", count);
                    partitionDetails.add(pMap);
                }

                Map<String, String> relevantConfigs = new LinkedHashMap<>();
                if (cfg != null) {
                    putConfigIfPresent(cfg, TopicConfig.RETENTION_MS_CONFIG, relevantConfigs);
                    putConfigIfPresent(cfg, TopicConfig.CLEANUP_POLICY_CONFIG, relevantConfigs);
                    putConfigIfPresent(cfg, TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, relevantConfigs);
                }

                Map<String, Object> item = new LinkedHashMap<>();
                item.put("name", name);
                item.put("partitions", partitionCount);
                item.put("replicas", replicationFactor);
                item.put("totalMessages", totalMessages);
                item.put("configs", relevantConfigs);
                item.put("partitionDetails", partitionDetails);
                response.add(item);
            }
            return response;
        } catch (Exception e) {
            throw new IllegalStateException("Error listando tópicos de Kafka: " + e.getMessage());
        }
    }

    public Map<String, Object> crearTopico(CreateKafkaTopicRequestDTO dto) {
        try {
            String topicName = dto.getName().trim();
            NewTopic newTopic = new NewTopic(topicName, dto.getPartitions(), dto.getReplicas());

            Map<String, String> configs = new HashMap<>();
            if (dto.getRetentionMs() != null) {
                configs.put(TopicConfig.RETENTION_MS_CONFIG, String.valueOf(dto.getRetentionMs()));
            }
            if (dto.getCleanupPolicy() != null && !dto.getCleanupPolicy().isBlank()) {
                configs.put(TopicConfig.CLEANUP_POLICY_CONFIG, dto.getCleanupPolicy().trim());
            }
            newTopic.configs(configs);

            adminClient.createTopics(List.of(newTopic)).all().get(8, TimeUnit.SECONDS);
            log.info("[KAFKA-ADMIN] Tópico '{}' creado con {} particiones y {} réplicas",
                    topicName, dto.getPartitions(), dto.getReplicas());

            Map<String, Object> res = new LinkedHashMap<>();
            res.put("operation", "CREATE_TOPIC");
            res.put("topic", topicName);
            res.put("partitions", dto.getPartitions());
            res.put("replicas", dto.getReplicas());
            res.put("configs", configs);
            res.put("message", "Tópico '" + topicName + "' creado exitosamente en el clúster Kafka");
            res.put("timestamp", Instant.now().toString());
            return res;
        } catch (Exception e) {
            throw new IllegalArgumentException("No se pudo crear el tópico '" + dto.getName() + "': " + e.getMessage());
        }
    }

    public Map<String, Object> actualizarConfiguracionTopico(String topicName, UpdateKafkaTopicConfigDTO dto) {
        validarNombreTopico(topicName);
        String trimmed = topicName.trim();

        try {
            List<AlterConfigOp> ops = new ArrayList<>();
            if (dto.getRetentionMs() != null) {
                ops.add(new AlterConfigOp(
                        new ConfigEntry(TopicConfig.RETENTION_MS_CONFIG, String.valueOf(dto.getRetentionMs())),
                        AlterConfigOp.OpType.SET
                ));
            }
            if (dto.getCleanupPolicy() != null && !dto.getCleanupPolicy().isBlank()) {
                ops.add(new AlterConfigOp(
                        new ConfigEntry(TopicConfig.CLEANUP_POLICY_CONFIG, dto.getCleanupPolicy().trim()),
                        AlterConfigOp.OpType.SET
                ));
            }

            if (!ops.isEmpty()) {
                ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, trimmed);
                adminClient.incrementalAlterConfigs(Map.of(resource, ops)).all().get(8, TimeUnit.SECONDS);
            }

            if (dto.getNewPartitionCount() != null) {
                TopicDescription current = adminClient.describeTopics(List.of(trimmed))
                        .allTopicNames().get(6, TimeUnit.SECONDS).get(trimmed);
                if (current != null && dto.getNewPartitionCount() > current.partitions().size()) {
                    adminClient.createPartitions(Map.of(trimmed, NewPartitions.increaseTo(dto.getNewPartitionCount())))
                            .all().get(8, TimeUnit.SECONDS);
                }
            }

            log.info("[KAFKA-ADMIN] Configuración actualizada para tópico '{}'", trimmed);

            Map<String, Object> res = new LinkedHashMap<>();
            res.put("operation", "UPDATE_TOPIC_CONFIG");
            res.put("topic", trimmed);
            res.put("message", "Configuración del tópico '" + trimmed + "' actualizada correctamente");
            res.put("timestamp", Instant.now().toString());
            return res;
        } catch (Exception e) {
            throw new IllegalArgumentException("Error actualizando el tópico '" + trimmed + "': " + e.getMessage());
        }
    }

    public Map<String, Object> eliminarTopico(String topicName) {
        validarNombreTopico(topicName);
        String trimmed = topicName.trim();

        try {
            adminClient.deleteTopics(List.of(trimmed)).all().get(8, TimeUnit.SECONDS);
            log.info("[KAFKA-ADMIN] Tópico '{}' eliminado del clúster Kafka", trimmed);

            Map<String, Object> res = new LinkedHashMap<>();
            res.put("operation", "DELETE_TOPIC");
            res.put("topic", trimmed);
            res.put("message", "Tópico '" + trimmed + "' eliminado correctamente");
            res.put("timestamp", Instant.now().toString());
            return res;
        } catch (Exception e) {
            throw new IllegalArgumentException("No se pudo eliminar el tópico '" + trimmed + "': " + e.getMessage());
        }
    }

    public List<Map<String, Object>> inspeccionarConsumerGroups() {
        try {
            Collection<ConsumerGroupListing> listings =
                    adminClient.listConsumerGroups().all().get(6, TimeUnit.SECONDS);
            List<String> groupIds = listings.stream()
                    .map(ConsumerGroupListing::groupId)
                    .sorted()
                    .toList();

            if (groupIds.isEmpty()) {
                return Collections.emptyList();
            }

            Map<String, ConsumerGroupDescription> descriptions =
                    adminClient.describeConsumerGroups(groupIds).all().get(6, TimeUnit.SECONDS);

            List<Map<String, Object>> groupsInfo = new ArrayList<>();
            for (String groupId : groupIds) {
                ConsumerGroupDescription desc = descriptions.get(groupId);
                Map<TopicPartition, OffsetAndMetadata> committedOffsets =
                        adminClient.listConsumerGroupOffsets(groupId)
                                .partitionsToOffsetAndMetadata()
                                .get(6, TimeUnit.SECONDS);

                Map<TopicPartition, OffsetSpec> endSpecs = new HashMap<>();
                for (TopicPartition tp : committedOffsets.keySet()) {
                    endSpecs.put(tp, OffsetSpec.latest());
                }

                Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> endOffsets =
                        endSpecs.isEmpty()
                                ? Collections.emptyMap()
                                : adminClient.listOffsets(endSpecs).all().get(6, TimeUnit.SECONDS);

                long totalLag = 0L;
                List<Map<String, Object>> partitionLagList = new ArrayList<>();
                for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : committedOffsets.entrySet()) {
                    TopicPartition tp = entry.getKey();
                    long currentOffset = entry.getValue().offset();
                    long endOffset = endOffsets.containsKey(tp) ? endOffsets.get(tp).offset() : currentOffset;
                    long lag = Math.max(0L, endOffset - currentOffset);
                    totalLag += lag;

                    Map<String, Object> lagItem = new LinkedHashMap<>();
                    lagItem.put("topic", tp.topic());
                    lagItem.put("partition", tp.partition());
                    lagItem.put("currentOffset", currentOffset);
                    lagItem.put("endOffset", endOffset);
                    lagItem.put("lag", lag);
                    partitionLagList.add(lagItem);
                }

                List<Map<String, Object>> members = new ArrayList<>();
                for (MemberDescription member : desc.members()) {
                    Map<String, Object> m = new LinkedHashMap<>();
                    m.put("consumerId", member.consumerId());
                    m.put("clientId", member.clientId());
                    m.put("host", member.host());
                    m.put("assignedPartitions", member.assignment().topicPartitions().size());
                    members.add(m);
                }

                Map<String, Object> gMap = new LinkedHashMap<>();
                gMap.put("groupId", groupId);
                gMap.put("state", desc.state().toString());
                gMap.put("memberCount", desc.members().size());
                gMap.put("totalLag", totalLag);
                gMap.put("offsets", partitionLagList);
                gMap.put("members", members);
                groupsInfo.add(gMap);
            }
            return groupsInfo;
        } catch (Exception e) {
            throw new IllegalStateException("Error inspeccionando Consumer Groups: " + e.getMessage());
        }
    }

    private void putConfigIfPresent(Config cfg, String key, Map<String, String> target) {
        ConfigEntry entry = cfg.get(key);
        if (entry != null) {
            target.put(key, entry.value());
        }
    }

    private void validarNombreTopico(String topicName) {
        if (topicName == null || topicName.isBlank()) {
            throw new IllegalArgumentException("El nombre del tópico no puede estar vacío");
        }
        if (!topicName.trim().matches("^[a-zA-Z0-9._-]+$")) {
            throw new IllegalArgumentException("El nombre del tópico contiene caracteres inválidos");
        }
    }
}
