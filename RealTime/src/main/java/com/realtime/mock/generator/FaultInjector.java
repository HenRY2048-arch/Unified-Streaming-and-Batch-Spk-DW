package com.realtime.mock.generator;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;

/**
 * Kafka 发送前的事件缺陷注入器。
 *
 * <p>所有模式都在 {@code config.properties} 中显式开启。它只影响主事件流，不污染独立归因真值：
 * 这样才能观察 Data Agent 如何面对“转化事实存在，但主事件缺失/损坏”的真实故障形态。
 */
public final class FaultInjector {
    public static final class Payload {
        public final String key;
        public final String value;

        Payload(String key, String value) {
            this.key = key;
            this.value = value;
        }
    }

    private final ObjectMapper mapper = new ObjectMapper();
    private final Random random;
    private final RunManifest manifest;
    private final List<String> modes;

    public FaultInjector(Random random, RunManifest manifest) {
        this.random = random;
        this.manifest = manifest;
        String configured = AppConfig.DATA_DEFECT_MODES == null
                ? "" : AppConfig.DATA_DEFECT_MODES.trim();
        this.modes = configured.isEmpty()
                ? Collections.emptyList()
                : Arrays.asList(configured.split("\\s*,\\s*"));
    }

    public List<Payload> prepare(EventLog event) {
        if (AppConfig.DATA_DEFECT_ENABLED && random.nextDouble() < AppConfig.DATA_DEFECT_DROP_RATE) {
            manifest.injected("drop", 1);
            return Collections.emptyList();
        }

        String json = toJson(event);
        if (AppConfig.DATA_DEFECT_ENABLED && random.nextDouble() < AppConfig.DATA_DEFECT_RATE) {
            json = corrupt(event, json);
        }

        String key = String.valueOf(event.getUserId());
        List<Payload> result = new ArrayList<>(2);
        result.add(new Payload(key, json));
        if (AppConfig.DATA_DEFECT_ENABLED
                && random.nextDouble() < AppConfig.DATA_DEFECT_DUPLICATE_RATE) {
            result.add(new Payload(key, json));
            manifest.injected("duplicate", 1);
        }
        return result;
    }

    private String corrupt(EventLog event, String validJson) {
        if (modes.isEmpty()) return validJson;
        String mode = modes.get(random.nextInt(modes.size()));
        manifest.injected(mode, 1);
        if ("invalid_json".equals(mode)) {
            return "{\"event_id\":\"" + event.getEventId() + "\",\"event_type\":";
        }
        try {
            JsonNode node = mapper.readTree(validJson);
            if (!(node instanceof ObjectNode)) return validJson;
            ObjectNode object = (ObjectNode) node;
            switch (mode) {
                case "missing_field":
                    object.remove("event_id");
                    break;
                case "invalid_enum":
                    object.put("event_type", "__invalid_event_type__");
                    break;
                case "wrong_type":
                    object.put("user_id", "__wrong_user_id__");
                    break;
                default:
                    break;
            }
            return mapper.writeValueAsString(object);
        } catch (Exception e) {
            manifest.injected("corruption_serialize_error", 1);
            return validJson;
        }
    }

    private String toJson(EventLog event) {
        try {
            return mapper.writeValueAsString(event);
        } catch (Exception e) {
            throw new IllegalStateException("cannot serialize event " + event.getEventId(), e);
        }
    }
}
