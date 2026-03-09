-- API Log Analytics Queries
-- These queries run on processed log data

-- 1. Overall error rate
SELECT 
    COUNT(*) as total_requests,
    SUM(CASE WHEN status_code >= 400 THEN 1 ELSE 0 END) as total_errors,
    ROUND(SUM(CASE WHEN status_code >= 400 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as error_rate_pct
FROM api_logs;

-- 2. Error rate by endpoint
SELECT 
    endpoint,
    COUNT(*) as total_requests,
    SUM(CASE WHEN status_code >= 400 THEN 1 ELSE 0 END) as error_count,
    ROUND(SUM(CASE WHEN status_code >= 400 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as error_rate_pct,
    ROUND(AVG(response_time_ms), 2) as avg_response_time_ms
FROM api_logs
GROUP BY endpoint
ORDER BY error_rate_pct DESC;

-- 3. Slowest endpoints
SELECT 
    endpoint,
    ROUND(AVG(response_time_ms), 2) as avg_response_time_ms,
    MAX(response_time_ms) as max_response_time_ms,
    MIN(response_time_ms) as min_response_time_ms
FROM api_logs
GROUP BY endpoint
ORDER BY avg_response_time_ms DESC;

-- 4. Status code distribution
SELECT 
    status_code,
    COUNT(*) as count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as percentage
FROM api_logs
GROUP BY status_code
ORDER BY status_code;

-- 5. Traffic by hour
SELECT 
    HOUR(timestamp) as hour,
    COUNT(*) as request_count,
    ROUND(AVG(response_time_ms), 2) as avg_response_time_ms
FROM api_logs
GROUP BY HOUR(timestamp)
ORDER BY hour;

-- 6. Top clients by error rate
SELECT 
    client_ip,
    COUNT(*) as total_requests,
    SUM(CASE WHEN status_code >= 400 THEN 1 ELSE 0 END) as error_count
FROM api_logs
GROUP BY client_ip
HAVING error_count > 0
ORDER BY error_count DESC
LIMIT 10;