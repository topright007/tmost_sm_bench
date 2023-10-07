package com.sparkdan.tmost_state_machine_bench;

import java.util.List;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import lombok.Builder;
import lombok.Data;
import org.joda.time.Instant;

@Data
@Builder
public class UpsertRMSRequest {
    @Nonnull
    String roomId;
    @Nonnull
    String roomSessionId;
    @Nonnull
    String peerId;
    List<RoomMediaSessionState> updatedStates;
    RoomMediaSessionState newState;
    @Nullable
    Instant newCreatedAt;
    @Nullable
    Instant newFirstOfferAt;
    @Nullable
    Instant newConnectedAt;
    @Nullable
    Instant newDisconnectedAt;
}
