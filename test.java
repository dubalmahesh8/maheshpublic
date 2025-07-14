@Service
@Slf4j
@RequiredArgsConstructor
public class JobsTracker {

    private static final String TOTAL_COUNT = ".total.count";
    private static final String TOTAL_DURATION = ".total.duration";
    private static final String AVG_DURATION = ".avg.duration";

    private final RedisTemplate<String, Object> redisTemplate;

    public void trackRunning(String jobId, boolean isRunning) {
        String runningKey = jobId + ".running";
        String firstKey = jobId + ".firstExecuted";
        String lastKey = jobId + ".lastExecuted";

        redisTemplate.opsForValue().set(runningKey, isRunning);

        LocalDateTime now = LocalDateTime.now();
        if (redisTemplate.opsForValue().get(firstKey) == null) {
            redisTemplate.opsForValue().set(firstKey, now.toString());
        }

        redisTemplate.opsForValue().set(lastKey, now.toString());
    }

    public boolean getIsRunning(String jobId) {
        Boolean running = (Boolean) redisTemplate.opsForValue().get(jobId + ".running");
        return Boolean.TRUE.equals(running);
    }

    public void trackDuration(String jobId, String operation, long duration) {
        String prefix = jobId.replace(".", "") + "." + operation.replace(".", "");
        String countKey = prefix + TOTAL_COUNT;
        String durationKey = prefix + TOTAL_DURATION;

        redisTemplate.opsForValue().increment(countKey, 1);
        redisTemplate.opsForValue().increment(durationKey, duration);
    }

    public void trackCount(String jobId, String operation, long count) {
        String countKey = jobId.replace(".", "") + "." + operation.replace(".", "") + ".count";
        redisTemplate.opsForValue().increment(countKey, count);
    }

    public Map<String, Number> getMetrics(String jobId) {
        String jobPrefix = jobId.replace(".", "");

        Set<String> keys = redisTemplate.keys(jobPrefix + "*");
        if (keys == null) return Collections.emptyMap();

        Map<String, Number> metrics = new HashMap<>();

        for (String key : keys) {
            Object value = redisTemplate.opsForValue().get(key);
            if (value instanceof Number) {
                metrics.put(key.replace(jobPrefix, ""), (Number) value);
            }
        }

        // Compute averages
        Map<String, Number> avgMetrics = new HashMap<>();
        for (String key : metrics.keySet()) {
            if (key.endsWith(TOTAL_COUNT)) {
                String prefix = key.replace(TOTAL_COUNT, "");
                Number totalDuration = metrics.getOrDefault(prefix + TOTAL_DURATION, 0L);
                Number totalCount = metrics.getOrDefault(key, 1L); // avoid divide by zero
                double avg = totalDuration.doubleValue() / totalCount.doubleValue();
                avgMetrics.put(prefix + AVG_DURATION, avg);
            }
        }

        metrics.putAll(avgMetrics);
        return metrics;
    }

    public Optional<LocalDateTime> getFirstExecuted(String jobId) {
        Object value = redisTemplate.opsForValue().get(jobId + ".firstExecuted");
        return value != null ? Optional.of(LocalDateTime.parse(value.toString())) : Optional.empty();
    }

    public Optional<LocalDateTime> getLastExecuted(String jobId) {
        Object value = redisTemplate.opsForValue().get(jobId + ".lastExecuted");
        return value != null ? Optional.of(LocalDateTime.parse(value.toString())) : Optional.empty();
    }

    // ✅ Inner helper class
    public class DurationTracker {

        private final String jobId;
        private final String operation;
        private final LocalDateTime startTime = LocalDateTime.now();

        public DurationTracker(String jobId, String operation) {
            this.jobId = jobId;
            this.operation = operation;
        }

        public long track() {
            long duration = startTime.until(LocalDateTime.now(), ChronoUnit.MILLIS);
            trackDuration(jobId, operation, duration);
            return duration;
        }
    }

    // Utility to create DurationTracker
    public DurationTracker trackDuration(String jobId, String operation) {
        return new DurationTracker(jobId, operation);
    }
}
