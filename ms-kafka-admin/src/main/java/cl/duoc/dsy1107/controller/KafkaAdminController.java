package cl.duoc.dsy1107.controller;

import cl.duoc.dsy1107.dto.CreateKafkaTopicRequestDTO;
import cl.duoc.dsy1107.dto.UpdateKafkaTopicConfigDTO;
import cl.duoc.dsy1107.service.KafkaAdminService;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@RestController
public class KafkaAdminController {

    private final KafkaAdminService kafkaAdminService;

    public KafkaAdminController(KafkaAdminService kafkaAdminService) {
        this.kafkaAdminService = kafkaAdminService;
    }

    @GetMapping({"/kafka/brokers", "/api/admin/kafka/brokers"})
    public ResponseEntity<Map<String, Object>> obtenerBrokers() {
        return ResponseEntity.ok(kafkaAdminService.obtenerEstadoClusterYBrokers());
    }

    @GetMapping({"/kafka/topics", "/api/admin/kafka/topics"})
    public ResponseEntity<List<Map<String, Object>>> listarTopicos() {
        return ResponseEntity.ok(kafkaAdminService.listarTopicosConDetalle());
    }

    @PostMapping({"/kafka/topics", "/api/admin/kafka/topics"})
    public ResponseEntity<Map<String, Object>> crearTopico(
            @Valid @RequestBody CreateKafkaTopicRequestDTO request) {
        return ResponseEntity.status(HttpStatus.CREATED).body(kafkaAdminService.crearTopico(request));
    }

    @PutMapping({"/kafka/topics/{name}", "/api/admin/kafka/topics/{name}"})
    public ResponseEntity<Map<String, Object>> actualizarConfiguracionTopico(
            @PathVariable("name") String name,
            @Valid @RequestBody UpdateKafkaTopicConfigDTO request) {
        return ResponseEntity.ok(kafkaAdminService.actualizarConfiguracionTopico(name, request));
    }

    @DeleteMapping({"/kafka/topics/{name}", "/api/admin/kafka/topics/{name}"})
    public ResponseEntity<Map<String, Object>> eliminarTopico(@PathVariable("name") String name) {
        return ResponseEntity.ok(kafkaAdminService.eliminarTopico(name));
    }

    @GetMapping({"/kafka/consumer-groups", "/api/admin/kafka/consumer-groups"})
    public ResponseEntity<List<Map<String, Object>>> listarConsumerGroups() {
        return ResponseEntity.ok(kafkaAdminService.inspeccionarConsumerGroups());
    }
}
