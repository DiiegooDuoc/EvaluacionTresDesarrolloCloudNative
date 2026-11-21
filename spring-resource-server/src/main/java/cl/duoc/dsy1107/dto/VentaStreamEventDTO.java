package cl.duoc.dsy1107.dto;

import jakarta.validation.constraints.Email;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class VentaStreamEventDTO {

    private String eventId;
    private Long pedidoId;

    @NotBlank(message = "El correo del cliente es obligatorio")
    @Email(message = "El correo del cliente debe ser válido")
    private String clienteEmail;

    @NotNull(message = "El identificador del producto es obligatorio")
    private Long productoId;

    @NotBlank(message = "El nombre del producto es obligatorio")
    private String productoNombre;

    @NotBlank(message = "La categoría del producto es obligatoria para el particionamiento")
    private String categoria;

    @NotNull(message = "La cantidad vendida es obligatoria")
    @Min(value = 1, message = "La cantidad mínima es 1")
    private Integer cantidad;

    @NotNull(message = "El monto total de la venta es obligatorio")
    @Positive(message = "El monto total debe ser mayor que cero")
    private Double montoTotal;

    @NotBlank(message = "El tipo de documento tributario es obligatorio")
    private String tipoDocumento;

    @Builder.Default
    private String region = "RM-SANTIAGO";

    private boolean simularErrorProcesamiento;
    private LocalDateTime timestamp;
}
