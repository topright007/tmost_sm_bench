create type room_media_session_state as enum (
    'CREATED',
    'FIRST_OFFER_RECEIVED',
    'CONNECTED',
    'DISCONNECTED',
    'ARCHIVED'
    );

CREATE CAST (character varying AS room_media_session_state) WITH INOUT AS IMPLICIT;

create table rooms (room_id text primary key, current_room_session_id text);

create table room_media_sessions
(
    peer_id text                     not null,
    room_session_id  text                     not null,
    room_id          text                     not null references rooms,
    created_at       timestamptz(3)           not null,
    first_offer_at   timestamptz(3),
    connected_at     timestamptz(3),
    disconnected_at  timestamptz(3),
    state            room_media_session_state not null,

    CONSTRAINT pk_room_media_sessions_id PRIMARY KEY (room_session_id, peer_id),

    CONSTRAINT check_room_media_sessions_state_requirements CHECK (
                state = 'ARCHIVED' or
                (state = 'CREATED' and room_session_id = 'rms_unknown'
                    and created_at is not null and first_offer_at is null
                    and connected_at is null and disconnected_at is null) or
                (state = 'FIRST_OFFER_RECEIVED' and room_session_id <> 'rms_unknown'
                    and created_at is not null and first_offer_at is not null
                    and connected_at is null and disconnected_at is null) or
                (state = 'CONNECTED' and room_session_id <> 'rms_unknown'
                    and created_at is not null
                    and connected_at is not null and disconnected_at is null) or
                (state = 'DISCONNECTED' and room_session_id <> 'rms_unknown'
                    and created_at is not null and disconnected_at is not null
                    )
        )
);

CREATE INDEX if not exists idx_room_media_sessions_media_session_id ON room_media_sessions (peer_id);
CREATE INDEX if not exists idx_room_media_sessions_room_id ON room_media_sessions (room_id, peer_id);

--this one is to quickly find stalled active sessions and deactivate them. validation is needed on every offer
CREATE INDEX if not exists idx_room_media_sessions_active_sessions ON room_media_sessions
    (room_id, room_session_id, created_at)
    where disconnected_at is null
        and state in ('CREATED', 'FIRST_OFFER_RECEIVED', 'CONNECTED')
;

--this one is to validate if session being connected is present among stalled sessions
CREATE INDEX if not exists idx_room_media_sessions_not_archived ON room_media_sessions
    (room_id, room_session_id, created_at)
    where state not in ('ARCHIVED')
;

