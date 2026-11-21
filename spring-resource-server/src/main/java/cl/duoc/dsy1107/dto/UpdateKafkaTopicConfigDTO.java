package cl.duoc.dsy1107.dto;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.Pattern;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class UpdateKafkaTopicConfigDTO {

    @Min(value = 60000, message = "El tiempo mínimo de retención es 60000 ms (1 minuto)")
    private Long retentionMs;

    @Pattern(
        regexp = "^(delete|compact|compact,delete)$",
        message = "La política cleanup.policy debe ser 'delete', 'compact' o 'compact,delete'"
    )
    private String cleanupPolicy;

    @Min(value = 1, message = "El número total de particiones debe ser mayor o igual al actual")
    @Max(value = 24, message = "El número máximo de particiones permitido es 24")
    private Integer newPartitionCount;
}
