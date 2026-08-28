package com.cloudrelay.dto;

import jakarta.validation.constraints.NotBlank;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class MatchRequest implements Serializable {

    private static final long serialVersionUID = 1L;

    @NotBlank
    private String playerId;

    @NotBlank
    private String displayName;

    @NotBlank
    private String gameId;

    @NotBlank
    private String region;
}
