package com.cloudrelay.dto;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class CreateSessionRequest {

    @NotBlank
    private String gameId;

    @NotBlank
    private String playerId;

    @NotBlank
    private String displayName;

    @NotBlank
    private String region;

    @Min(2)
    @Max(16)
    private Integer maxPlayers;

    @Min(2)
    private Integer minPlayersToStart;

    private Map<String, Object> metadata;
}
