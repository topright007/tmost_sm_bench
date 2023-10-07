package com.sparkdan.tmost_state_machine_bench;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;

@Service
@Slf4j
public class RunTests {

    @Value("${num.peers}")
    public int numPeers;

    @Value("${test.duration.millis}")
    public long testDurationMillis;

    @Value("${test.concurrency}")
    public long testConcurrency;

    @Value("${test.pause.between.samples.millis}")
    public long pauseBetweenSamplesMillis;

    @Autowired
    protected SampleService sampleService;

    @Autowired
    protected RoomMediaSessionDao dao;

    private RestTemplate restTemplate = new RestTemplate();

    private ExecutorService executor = Executors.newCachedThreadPool();

    private final ObjectMapper objectMapper = new ObjectMapper();

    @RequiredArgsConstructor
    static class Conf {
        private final String roomId;
        private final List<String> peerIDs = Collections.synchronizedList(new ArrayList<>());
        private final List<Future<String>> hangingOffers = Collections.synchronizedList(new ArrayList<>());
    }

    public void runTests() throws InterruptedException, IOException {
        String runVariations = System.getProperty("variations");
        runSuit(false, 0);

//        if(StringUtils.isBlank(runVariations)) {
//            return;
//        }

        sleepBetweenTests();
        runSuit(true, 0);
        sleepBetweenTests();

        runSuit(false, 1);
        sleepBetweenTests();
        runSuit(true, 1);
//        sleepBetweenTests();
//
//        runSuit(true, 1, 10);
//        sleepBetweenTests();
//        runSuit(false, 1, 10);
//        sleepBetweenTests();
//
//        runSuit(false, 30);
//        sleepBetweenTests();
//        runSuit(true, 30);
    }

    private void sleepBetweenTests() throws InterruptedException {
        log.info("Sleeping for {} ms between tests", pauseBetweenSamplesMillis);
        Thread.sleep(pauseBetweenSamplesMillis);
    }

    public void runSuit(boolean useLocks, long pgPingMs) throws InterruptedException, IOException {
        runSuit(useLocks, pgPingMs, 0);
    }

    public void runSuit(boolean useLocks, long pgPingMs, long throttleRPSDelay) throws InterruptedException, IOException {
        log.info("Running tests. Locks: {}, pgPing: {} ms, Throttle RPS delay {}", useLocks, pgPingMs, throttleRPSDelay);
        setPgPing(pgPingMs);
        sampleService.setUseLocks(useLocks);
        sampleService.setRpsDelay(throttleRPSDelay);

        long startMs = System.currentTimeMillis();
        launchCycles().join();
        long endMs = System.currentTimeMillis();

        outputResult(useLocks, pgPingMs, startMs, endMs, throttleRPSDelay);
        log.info("Finished running tests. Locks: {}, pgPing: {} ms, Throttle RPS delay {}", useLocks, pgPingMs, throttleRPSDelay);
    }

    private void queryPrometheusToCSV(String query, long startMs, long endMs, String csvPath, String csvHeaders) throws IOException {
        URI resultsUri = UriComponentsBuilder.fromHttpUrl("http://localhost:9090/api/v1/query_range")
                .queryParam("query", query, StandardCharsets.UTF_8)
                .queryParam("start", dottedSeconds(startMs + 5000))
                .queryParam("end", dottedSeconds(endMs))
                .queryParam("step", "1")
                .build().toUri();
        String resultStr = restTemplate.getForObject(resultsUri, String.class);
        JsonNode result = objectMapper.readTree(resultStr);
        ArrayNode valuesArr = (ArrayNode) result.at("/data/result/0/values");

        StringBuilder csvBuilder = new StringBuilder(StringUtils.trim(csvHeaders) + "\n");
        valuesArr.elements().forEachRemaining(node -> {
            double timestampSecs = node.get(0).asDouble();
            long timestampMs = (long) ( timestampSecs * 1000.0);
            String value = node.get(1).asText();
            csvBuilder.append(timestampMs).append(",").append(value).append("\n");
        });

        File targetFile = new File(csvPath);
        FileUtils.writeStringToFile(
                targetFile,
                csvBuilder.toString(),
                StandardCharsets.UTF_8
        );
    }

    private void outputResult(boolean useLocks, long pgPingMS, long startMs, long endMs, long throttleRPSDelay ) throws IOException {
        queryPrometheusToCSV(
                "rate(sampleservice_callsConnected_total[5s])",
                startMs,
                endMs,
                String.format(
                        "ipnb/cps_%b+ping_%d_rps_%d.csv",
                        useLocks,
                        pgPingMS,
                        throttleRPSDelay
                ),
                "time,rps"
        );
        queryPrometheusToCSV(
                "hikaricp_connections_active",
                startMs,
                endMs,
                String.format(
                        "ipnb/connections_%b+ping_%d_rps_%d.csv",
                        useLocks,
                        pgPingMS,
                        throttleRPSDelay
                ),
                "time,rps"
        );
    }

    private String dottedSeconds(long millis) {
        long seconds = millis / 1000;
        long remainder = millis % 1000;
        return seconds + "." + remainder;
    }

    private void setPgPing(long ms) throws IOException {
        log.info("dropping previous pg delay setting");
        //drop delay
        try {
            CommandLine oCmdLine = CommandLine.parse(
                    "docker exec postgres tc qdisc del dev eth0 root netem delay 1ms"
            );
            DefaultExecutor oDefaultExecutor = new DefaultExecutor();
            oDefaultExecutor.setExitValue(0);
            oDefaultExecutor.execute(oCmdLine);
        } catch (Exception e) {
            log.error("failed to drop PG delay. proceeding with the execution", e);
        }

        if(ms == 0) {
            log.info("Not setting new PG delay");
            return;
        }

        log.info("setting pg delay to {}", ms);
        CommandLine oCmdLine = CommandLine.parse(String.format(
                "docker exec postgres tc qdisc add dev eth0 root netem delay %dms",
                ms
        ));
        DefaultExecutor oDefaultExecutor = new DefaultExecutor();
        oDefaultExecutor.setExitValue(0);
        oDefaultExecutor.execute(oCmdLine);
    }

    public CompletableFuture<Void> launchCycles() {
        long start = System.currentTimeMillis();
        List<CompletableFuture<Void>> cycles = new ArrayList<>();
        for (int i = 0; i < testConcurrency; i++) {
            cycles.add(CompletableFuture.runAsync(() -> {
                while (System.currentTimeMillis() - start < testDurationMillis) {
                    mainOneCycle();
                }
            }, executor));
        }

        return CompletableFuture.allOf(cycles.toArray(new CompletableFuture[0]));
    }

    public void mainOneCycle() {
        try {
            String roomId = UUID.randomUUID().toString();
            dao.createRoom(roomId);
            log.trace("Running cycle with room: {}", roomId);
            Conf conf = new Conf(roomId);

            String roomSession1 = UUID.randomUUID().toString();
            everyoneJoins(conf, roomSession1);

            String roomSession2 = UUID.randomUUID().toString();

            sampleService.offerReceived(conf.roomId, conf.peerIDs.get(0), roomSession2);
            sampleService.connected(conf.roomId, conf.peerIDs.get(0), roomSession2);

            //when everyone idles in first media session, they're going to receive non-legitimate offers
            CompletableFuture<Void> allDisconnected = everyoneIdlyDisconnectsAsync(conf, roomSession1);
            conf.peerIDs.clear();

            everyoneJoins(conf, roomSession2);
            allDisconnected.join();
        } catch (Exception e) {
            log.error("Exception in test cycle", e);
        }
    }

    public CompletableFuture<Void> everyoneIdlyDisconnectsAsync(Conf conf, String roomSessionId) {
        List<CompletableFuture<Void>> leaves = new ArrayList<>();
        List<String> localPeerIDs = new ArrayList<>(conf.peerIDs);
        for (String peerId : localPeerIDs) {
            leaves.add(CompletableFuture.runAsync(() -> {
                        sampleService.offerReceived(conf.roomId, peerId, roomSessionId);
                        sampleService.disconnected(conf.roomId, peerId, roomSessionId);
                    },
                    executor
            ));
        }
        return CompletableFuture.allOf(leaves.toArray(new CompletableFuture[0]));
    }

    public void everyoneJoins(Conf conf, String roomSessionId) {
        List<Future<String>> joins = new ArrayList<>();
        for (int i = 0; i < numPeers; i++) {
            joins.add(joinPeer(conf, roomSessionId));
        }
        waitFutures(joins);
        waitFutures(conf.hangingOffers);
        conf.hangingOffers.clear();
    }

    private <T> void waitFutures(List<Future<T>> futures) {
        for (Future<?> f : futures) {
            try {
                f.get();
            } catch (ExecutionException | InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public Future<String> joinPeer(Conf conf, String roomSessionId) {
        return executor.submit(() -> {
            String peerId = UUID.randomUUID().toString();
            sampleService.createSession(conf.roomId, peerId);
            sampleService.offerReceived(conf.roomId, peerId, roomSessionId);

            offerToAllAsync(conf, roomSessionId);

            sampleService.connected(conf.roomId, peerId, roomSessionId);
            conf.peerIDs.add(peerId);
            return peerId;
        });
    }

    public void offerToAllAsync(Conf conf, String roomSessionId) {
        List<String> presentPeers = new ArrayList<>(conf.peerIDs);
        for (String presentPeer : presentPeers) {
            conf.hangingOffers.add(executor.submit(() -> {
                sampleService.offerReceived(conf.roomId, presentPeer, roomSessionId);
                return "";
            }));
        }

    }


}
