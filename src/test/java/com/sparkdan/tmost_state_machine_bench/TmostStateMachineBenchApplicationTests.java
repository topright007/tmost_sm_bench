package com.sparkdan.tmost_state_machine_bench;

import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import static com.sparkdan.tmost_state_machine_bench.RoomMediaSessionDao.UNKNOWN_ROOM_SESSION_ID;
import static com.sparkdan.tmost_state_machine_bench.RoomMediaSessionState.CONNECTED;
import static com.sparkdan.tmost_state_machine_bench.RoomMediaSessionState.CREATED;
import static com.sparkdan.tmost_state_machine_bench.RoomMediaSessionState.DISCONNECTED;
import static com.sparkdan.tmost_state_machine_bench.RoomMediaSessionState.FIRST_OFFER_RECEIVED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;


@SpringBootTest
class TmostStateMachineBenchApplicationTests {

    @Autowired
    protected SampleService sampleService;

    @Autowired
    protected RoomMediaSessionDao dao;

    @Test
    void persistWorks() {
        String roomId = UUID.randomUUID().toString();
        String peerId = UUID.randomUUID().toString();
        String roomSessionId = UUID.randomUUID().toString();
        dao.createRoom(roomId);
        sampleService.createSession(roomId, peerId);

        RoomMediaSessionDto dto = dao.findByPrimaryKey(UNKNOWN_ROOM_SESSION_ID, peerId);
        assertNotNull(dto);
        assertEquals(CREATED, dto.getState());

        sampleService.offerReceived(roomId, peerId, roomSessionId);
        dto = dao.findByPrimaryKey(roomSessionId, peerId);
        assertNotNull(dto);
        assertEquals(FIRST_OFFER_RECEIVED, dto.getState());

        sampleService.connected(roomId, peerId, roomSessionId);
        dto = dao.findByPrimaryKey(roomSessionId, peerId);
        assertNotNull(dto);
        assertEquals(CONNECTED, dto.getState());

        sampleService.offerReceived(roomId, peerId, roomSessionId);
        dto = dao.findByPrimaryKey(roomSessionId, peerId);
        assertNotNull(dto);
        assertEquals(CONNECTED, dto.getState());

        sampleService.disconnected(roomId, peerId, roomSessionId);
        dto = dao.findByPrimaryKey(roomSessionId, peerId);
        assertNotNull(dto);
        assertEquals(DISCONNECTED, dto.getState());

    }

    @Test
    public void testReconnectSignalled(){
        String roomId = UUID.randomUUID().toString();
        String peer1 = UUID.randomUUID().toString();
        String peer2 = UUID.randomUUID().toString();
        String discoPeer = UUID.randomUUID().toString();
        String roomSession1 = UUID.randomUUID().toString();

        dao.createRoom(roomId);

        sampleService.connected(roomId, peer1, roomSession1);
        sampleService.connected(roomId, peer2, roomSession1);
        sampleService.disconnected(roomId, discoPeer, roomSession1);

        assertState(peer1, roomSession1, CONNECTED);
        assertState(peer2, roomSession1, CONNECTED);
        assertState(discoPeer, roomSession1, DISCONNECTED);

        String roomSession2 = UUID.randomUUID().toString();
        sampleService.offerReceived(roomId, peer1, roomSession2);

        assertState(peer1, roomSession1, DISCONNECTED);
        assertState(peer2, roomSession1, DISCONNECTED);

        assertNotExists(discoPeer, UNKNOWN_ROOM_SESSION_ID);
        assertState(peer1, roomSession2, FIRST_OFFER_RECEIVED);
        assertState(peer2, UNKNOWN_ROOM_SESSION_ID, CREATED);

    }

    private void assertNotExists(String peerId, String roomSessionId) {
        RoomMediaSessionDto dto = dao.findByPrimaryKey(roomSessionId, peerId);
        assertNull(dto);
    }

    private void assertState(String peerId, String roomSessionId, RoomMediaSessionState state) {
        RoomMediaSessionDto dto = dao.findByPrimaryKey(roomSessionId, peerId);
        assertNotNull(dto);
        assertEquals(state, dto.getState());
    }


}
