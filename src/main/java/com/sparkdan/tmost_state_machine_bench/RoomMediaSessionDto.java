package com.sparkdan.tmost_state_machine_bench;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.ToString;
import org.jetbrains.annotations.Nullable;
import org.joda.time.Instant;

@Data
@ToString(doNotUseGetters = true)
@Builder
@AllArgsConstructor
public class RoomMediaSessionDto {
    private String roomId;
    private String mediaSessionId;
    @Nullable
    private String roomSessionId;

    private Instant createdAt;
    @Nullable
    private Instant firstOfferAt;
    @Nullable
    private Instant connectedAt;
    @Nullable
    private Instant disconnectedAt;

    private RoomMediaSessionState state;

    public boolean pastFirstOffer() {
        return firstOfferAt != null || pastConnected();
    }

    public boolean pastConnected() {
        return connectedAt != null || pastDisconnected();
    }

    public boolean pastDisconnected() {
        return disconnectedAt != null;
    }
}