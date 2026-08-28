package com.cloudrelay.dto;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
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

    /**
     * Elo-like skill rating, used only by the skill based matcher. Null means
     * unrated, which is treated as {@code WaitingPlayer.DEFAULT_RATING} rather
     * than as zero: an unrated player belongs in the middle of the
     * distribution, not at the bottom of it, or their first several matches
     * are against the weakest players on the platform.
     */
    @Min(0)
    @Max(4000)
    private Integer skillRating;
}
