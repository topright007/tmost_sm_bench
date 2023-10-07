package com.sparkdan.tmost_state_machine_bench;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import jakarta.annotation.Nonnull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.intellij.lang.annotations.Language;
import org.jetbrains.annotations.Nullable;
import org.joda.time.Instant;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;

@Service
@RequiredArgsConstructor
@Slf4j
public class RoomMediaSessionDao {

    public static final String COL_ROOM_ID = "room_id";
    public static final String COL_ROOM_SESSION_ID = "room_session_id";
    public static final String COL_PEER_ID = "peer_id";
    public static final String COL_CREATED_AT = "created_at";
    public static final String COL_FIRST_OFFER_AT = "first_offer_at";
    public static final String COL_CONNECTED_AT = "connected_at";
    public static final String COL_DISCONNECTED_AT = "disconnected_at";
    public static final String COL_STATE = "state";

    public static final String UNKNOWN_ROOM_SESSION_ID = "rms_unknown";

    private static final List<String> COLUMNS = List.of(COL_PEER_ID, COL_ROOM_SESSION_ID,
            COL_ROOM_ID, COL_CREATED_AT, COL_FIRST_OFFER_AT, COL_CONNECTED_AT, COL_DISCONNECTED_AT, COL_STATE);
    private static final String COLUMNS_STR = StringUtils.join(COLUMNS, ",");
    private static final String COLUMNS_BOUND_STR = boundColumns(COLUMNS);

    private static final List<String> CREATE_COLUMNS = List.of(COL_STATE, COL_ROOM_ID, COL_ROOM_SESSION_ID,
            COL_PEER_ID, COL_CREATED_AT);
    private static final String CREATE_COLUMNS_STR = StringUtils.join(CREATE_COLUMNS, ",");
    private static final String CREATE_COLUMNS_BOUND_STR = boundColumns(CREATE_COLUMNS);

    @Language("SQL")
    public static final String UPDATE_BASE_SQL = """
            update room_media_sessions
                set created_at = case when :created_at > created_at
                        then created_at else coalesce(:created_at, created_at) end,
                    first_offer_at = case when :first_offer_at > first_offer_at
                        then first_offer_at else coalesce(:first_offer_at, first_offer_at) end,
                    connected_at = case when :connected_at > connected_at
                        then connected_at else coalesce(:connected_at, connected_at) end,
                    disconnected_at = case when :disconnected_at > disconnected_at
                        then disconnected_at else coalesce(:disconnected_at, disconnected_at) end,
                    state = cast( case when cast(state as text) in (:updated_states)
                        then :state else cast(state as text) end
                        as room_media_session_state
                        ),
                    room_session_id = :room_session_id
            where peer_id = :peer_id
            """;
    @Language("SQL")
    public static final String UPDATE_BY_ROOM_SESSION_ID = UPDATE_BASE_SQL + """
                and room_session_id = :room_session_id
            """;
    @Language("SQL")
    public static final String UPDATE_CREATED_ROOM_SESSION = UPDATE_BASE_SQL + String.format("""
                and state = 'CREATED'
                and room_session_id = '%s'
            """, UNKNOWN_ROOM_SESSION_ID);
    @Language("SQL")
    public static final String INSERT_OR_DO_NOTHING = String.format("""
            insert into room_media_sessions (%s)
            values (%s)
            on conflict (room_session_id, peer_id) do nothing
            """, COLUMNS_STR, COLUMNS_BOUND_STR
    );

    private final JdbcTemplate jdbcTemplate;
    private final NamedParameterJdbcTemplate namedJdbcTemplate;

    private static String boundColumns(List<String> columns) {
        return columns.stream().map(c -> ":" + c).collect(Collectors.joining(","));
    }


    @Nullable
    public RoomMediaSessionDto findByPrimaryKey(@Nullable String roomSessionId, String peer_id) {
        List<RoomMediaSessionDto> result = jdbcTemplate.query("""
                        select * from room_media_sessions
                        where room_session_id = ?
                          and peer_id = ?
                        """,
                (rs, i) -> this.parseRow(rs),
                ObjectUtils.defaultIfNull(roomSessionId, UNKNOWN_ROOM_SESSION_ID),
                peer_id
        );
        if(CollectionUtils.isEmpty(result)) {
            return null;
        }
        return result.get(0);
    }

    @Nullable
    public String getLatestRoomSessionId(String roomId) {
        return jdbcTemplate.queryForObject("""
                        select current_room_session_id
                        from rooms
                        where
                            room_id = ?
                        """,
                String.class,
                roomId
        );
    }

    public void setLatestRoomSessionId(@Nonnull String roomId, @Nonnull String roomSessionId) {
        jdbcTemplate.update("""
                                update rooms
                                    set current_room_session_id = ?
                                where room_id = ?
                        """,
                roomSessionId,
                roomId
        );
    }

    public boolean isBrandNewRoomSession(String roomId, String roomSessionId) {
        Integer found = jdbcTemplate.queryForObject("""
                        select count(1)
                        from room_media_sessions
                        where room_id = ?
                          and room_session_id = ?
                          --just like in idx_room_media_sessions_not_archived
                          and state not in ('ARCHIVED', 'CREATED')
                        """,
                Integer.class,
                roomId, roomSessionId);
        return found == null || found == 0;
    }

    public void createRoom(String roomId) {
        jdbcTemplate.update("insert into rooms (room_id) values (?)", roomId);
    }

    public void created(String roomId, String peerId) {
        namedJdbcTemplate.update(String.format("""
                        insert into room_media_sessions (%s)
                        values (%s)
                        on conflict(peer_id, room_session_id) do nothing
                        """, CREATE_COLUMNS_STR, CREATE_COLUMNS_BOUND_STR),
                new MapSqlParameterSource(Map.of(
                        COL_ROOM_ID, roomId,
                        COL_ROOM_SESSION_ID, UNKNOWN_ROOM_SESSION_ID,
                        COL_PEER_ID, peerId,
                        COL_CREATED_AT, new Timestamp(Instant.now().toDate().getTime()),
                        COL_STATE, RoomMediaSessionState.CREATED.toString()
                )));
    }

    public Collection<RoomMediaSessionDto> disconnectAllInRoomAndRecreate(
            Collection<String> roomSessionIdsToDisconnect,
            String newRoomSessionId) {
        if (roomSessionIdsToDisconnect.size() == 0) {
            return Collections.emptyList();
        }

        Map<String, Object> params = Map.of(
                "room_session_id_to_disconnect", roomSessionIdsToDisconnect,
                "new_room_session_id", newRoomSessionId,
                "now", new Timestamp(System.currentTimeMillis()),
                "unknown_rms_id_constant", UNKNOWN_ROOM_SESSION_ID
        );
        return namedJdbcTemplate.query("""
                        -- selecting media sessions that need to be disconnected right now
                        -- because they have wrong room_media_session_id
                        with to_disconnect as (
                            select CTID as the_ctid --changes only on vacuum full which locks table exclusively
                                        --and won't give this lock
                            from room_media_sessions
                            where room_session_id in (:room_session_id_to_disconnect)
                              and state in ('FIRST_OFFER_RECEIVED', 'CONNECTED')
                              and disconnected_at is null
                            --this order by is needed to prevent deadlocks
                            order by room_session_id, peer_id
                            for update
                        ),
                        being_disconnected as (
                            update room_media_sessions
                            set disconnected_at = :now,
                                state = 'DISCONNECTED'
                            where CTID in (select the_ctid from to_disconnect)
                            returning peer_id, room_id
                        )
                        -- inserting 'CREATED' counterparts that indicate that the reconnect signal
                        -- is issued to media sessions in question
                        insert into room_media_sessions (
                            peer_id,
                            room_session_id,
                            room_id,
                            created_at,
                            state
                            )
                        -- double check that we're not issuing a reconnect signal
                        -- to sessions that are have already started connecting to the correct room_session_id
                        select distinct
                            peer_id,
                            :unknown_rms_id_constant,
                            room_id,
                            :now::timestamptz,
                            'CREATED'::room_media_session_state
                        from being_disconnected as bd where not exists (
                            select 1 from room_media_sessions as rms
                            where ((rms.room_session_id = :new_room_session_id
                                        and rms.state in ('FIRST_OFFER_RECEIVED', 'CONNECTED', 'DISCONNECTED'))
                                or (rms.room_session_id = :unknown_rms_id_constant and rms.state in ('CREATED'))
                            )
                            and rms.peer_id = bd.peer_id
                        )
                        --if some other query initiated the session before us
                        on conflict (room_session_id, peer_id) do nothing
                        returning *
                        """,
                params,
                (rs, rn) -> parseRow(rs)
        );
    }

    public RoomMediaSessionDto insert(RoomMediaSessionDto roomMediaSessionDto) {
        String roomSessionId = ObjectUtils.defaultIfNull(
                roomMediaSessionDto.getRoomSessionId(),
                UNKNOWN_ROOM_SESSION_ID
        );
        Map<String, Object> paramsMap = new HashMap<>();
        paramsMap.put(COL_ROOM_ID, roomMediaSessionDto.getRoomId());
        paramsMap.put(COL_ROOM_SESSION_ID, roomSessionId);
        paramsMap.put(COL_PEER_ID, roomMediaSessionDto.getMediaSessionId());
        paramsMap.put(COL_CREATED_AT, tmstmp(roomMediaSessionDto.getCreatedAt()));
        paramsMap.put(COL_FIRST_OFFER_AT, tmstmp(roomMediaSessionDto.getFirstOfferAt()));
        paramsMap.put(COL_CONNECTED_AT, tmstmp(roomMediaSessionDto.getConnectedAt()));
        paramsMap.put(COL_DISCONNECTED_AT, tmstmp(roomMediaSessionDto.getDisconnectedAt()));
        paramsMap.put(COL_STATE, roomMediaSessionDto.getState());

        return jdbcTemplate.queryForObject(String.format("""
                        insert into room_media_sessions (%s)
                        values (%s)
                        returning *
                        """, COLUMNS_STR, COLUMNS_BOUND_STR),
                (rs, num) -> parseRow(rs),
                paramsMap
        );
    }

    private Timestamp tmstmp(Instant instant) {
        if(instant == null) {
            return null;
        }
        return new Timestamp(instant.getMillis());
    }

    public Collection<String> findOtherActiveRoomSessions(@Nonnull String roomId, @Nonnull String roomSessionId) {
        return jdbcTemplate.queryForList("""
                        select distinct room_session_id
                        from room_media_sessions
                        where room_id = ?
                          and state in ('FIRST_OFFER_RECEIVED', 'CONNECTED')
                          and disconnected_at is null
                          and room_session_id <> ?
                        """,
                String.class,
                roomId,
                roomSessionId
        );
    }

    RoomMediaSessionDto parseRow(ResultSet rs) {
        try {
            String roomSessionId = rs.getString(COL_ROOM_SESSION_ID);
            if (UNKNOWN_ROOM_SESSION_ID.equals(roomSessionId)) {
                roomSessionId = null;
            }
            RoomMediaSessionDto result = RoomMediaSessionDto.builder()
                    .roomId(rs.getString(COL_ROOM_ID))
                    .roomSessionId(roomSessionId)
                    .mediaSessionId(rs.getString(COL_PEER_ID))
                    .createdAt(new Instant(rs.getTimestamp(COL_CREATED_AT)))
                    .firstOfferAt(Optional.ofNullable(rs.getTimestamp(COL_FIRST_OFFER_AT))
                            .map(Instant::new).orElse(null))
                    .connectedAt(Optional.ofNullable(rs.getTimestamp(COL_CONNECTED_AT))
                            .map(Instant::new).orElse(null))
                    .disconnectedAt(Optional.ofNullable(rs.getTimestamp(COL_DISCONNECTED_AT))
                            .map(Instant::new).orElse(null))
                    .state(RoomMediaSessionState.valueOf(rs.getString(COL_STATE)))
                    .build();
            return result;
        } catch (SQLException e) {
            throw new RuntimeException("Failed to load bridge media session", e);
        }
    }

    public boolean isConnected(String mediaSessionId) {
        Boolean result = jdbcTemplate.queryForObject("""
                        select count(1) > 0
                        from room_media_sessions
                        where peer_id = ?
                          and state = 'CONNECTED'
                        """,
                Boolean.class,
                mediaSessionId
        );
        Assert.notNull(result, "impossible");
        return result;
    }

    public void selectRoomIdForUpdate(String roomId) {
        jdbcTemplate.queryForObject("""
                select room_id from rooms where room_id = ? for update
                """,
                String.class,
                roomId
        );
    }

    public int updateByRoomSessionId(UpsertRMSRequest upsertRMSRequest) {
        MapSqlParameterSource params = fromUpsertRMSRequest(upsertRMSRequest);
        return namedJdbcTemplate.update(
                UPDATE_BY_ROOM_SESSION_ID,
                params
        );
    }

    public int updateCreatedRoomSession(UpsertRMSRequest upsertRMSRequest) {
        try {
            MapSqlParameterSource params = fromUpsertRMSRequest(upsertRMSRequest);
            return namedJdbcTemplate.update(
                    UPDATE_CREATED_ROOM_SESSION,
                    params
            );
        } catch (DataIntegrityViolationException e) {
            log.info("failed to update created RMS with update request {}. probably someone else already updated it",
                    upsertRMSRequest,
                    e
            );
            return 0;
        }
    }

    public int insertOrDoNothing(UpsertRMSRequest upsertRMSRequest) {
        MapSqlParameterSource params = fromUpsertRMSRequest(upsertRMSRequest);
        return namedJdbcTemplate.update(
                INSERT_OR_DO_NOTHING,
                params
        );
    }

    public int disconnectRoomMediaSessionsByMediaSessionId(String peerId) {
        @Language("SQL")
        final String query = """
            update room_media_sessions
            set state = 'DISCONNECTED', disconnected_at = :disconnected_at
            where (room_session_id, peer_id) in (
                select room_session_id, peer_id
                from room_media_sessions
                where peer_id = :peer_id and state in ('FIRST_OFFER_RECEIVED', 'CONNECTED')
                order by room_session_id, peer_id
                for update
            )
            """;

        Map<String, Object> params = new HashMap<>();
        params.put("peer_id", peerId);
        params.put("disconnected_at", tmstmp(Instant.now()));
        return namedJdbcTemplate.update(query, new MapSqlParameterSource(params));
    }

    public int disconnectRoomMediaSessionsByPeerId(String peerId) {

        @Language("SQL")
        final String query = """
            update room_media_sessions
            set state = 'DISCONNECTED', disconnected_at = :disconnected_at
            where peer_id in (
                select peer_id from room_media_sessions
                where peer_id = :peer_id
                order by room_session_id, peer_id
                for update
            )
            """;

        Map<String, Object> params = new HashMap<>();
        params.put("disconnected_at", tmstmp(Instant.now()));
        params.put("peer_id", peerId);
        return jdbcTemplate.update(query, params);
    }

    private MapSqlParameterSource fromUpsertRMSRequest(UpsertRMSRequest upsertRMSRequest) {
        Map<String, Object> result = new HashMap<>();

        result.put(COL_ROOM_ID, upsertRMSRequest.getRoomId());
        result.put(COL_ROOM_SESSION_ID, upsertRMSRequest.getRoomSessionId());
        result.put(COL_PEER_ID, upsertRMSRequest.getPeerId());
        result.put(COL_CREATED_AT, tmstmp(upsertRMSRequest.getNewCreatedAt()));
        result.put(COL_FIRST_OFFER_AT, tmstmp(upsertRMSRequest.getNewFirstOfferAt()));
        result.put(COL_CONNECTED_AT, tmstmp(upsertRMSRequest.getNewConnectedAt()));
        result.put(COL_DISCONNECTED_AT, tmstmp(upsertRMSRequest.getNewDisconnectedAt()));
        result.put(COL_STATE, upsertRMSRequest.getNewState().toString());
        result.put("updated_states", upsertRMSRequest.getUpdatedStates().stream().map(Object::toString).toList());

        return new MapSqlParameterSource(result);
    }




}
