package cl.duoc.dsy1107.dto;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class CreateKafkaTopicRequestDTO {

    @NotBlank(message = "El nombre del tópico no puede estar vacío")
    @Size(min = 3, max = 120, message = "El nombre del tópico debe tener entre 3 y 120 caracteres")
    @Pattern(
        regexp = "^[a-zA-Z0-9._-]+$",
        message = "El nombre del tópico solo puede contener letras, números, puntos, guiones y guiones bajos"
    )
    private String name;

    @Min(value = 1, message = "El número mínimo de particiones es 1")
    @Max(value = 24, message = "El número máximo permitido de particiones es 24")
    private int partitions = 3;

    @Min(value = 1, message = "El factor mínimo de replicación es 1")
    @Max(value = 3, message = "El factor máximo de replicación según los brokers disponibles es 3")
    private short replicas = 3;

    @Min(value = 60000, message = "El tiempo mínimo de retención es 60000 ms (1 minuto)")
    private Long retentionMs = 604800000L;

    @Pattern(
        regexp = "^(delete|compact|compact,delete)$",
        message = "La política cleanup.policy debe ser 'delete', 'compact' o 'compact,delete'"
    )
    private String cleanupPolicy = "delete";
}
