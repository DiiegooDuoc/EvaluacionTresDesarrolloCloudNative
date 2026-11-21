package cl.duoc.dsy1107.dto;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class InventarioStreamEventDTO {

    private String eventId;
    private Long pedidoId;

    @NotNull(message = "El ID del producto es obligatorio")
    private Long productoId;

    @NotBlank(message = "El nombre del producto es obligatorio")
    private String productoNombre;

    @NotBlank(message = "El tipo de movimiento es obligatorio (RESERVA_PEDIDO, AJUSTE_STOCK, REPOSICION)")
    private String tipoMovimiento;

    @NotNull(message = "Las unidades afectadas son obligatorias")
    @Min(value = 1, message = "Las unidades deben ser al menos 1")
    private Integer unidades;

    @NotNull(message = "El stock resultante es obligatorio")
    @Min(value = 0, message = "El stock disponible no puede ser negativo")
    private Integer stockDisponible;

    @Builder.Default
    private String bodega = "BODEGA-CENTRAL-SCL";

    @NotBlank(message = "El estado operacional es obligatorio")
    private String estadoOperacional;

    private boolean simularErrorProcesamiento;
    private LocalDateTime timestamp;
}
