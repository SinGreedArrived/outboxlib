-- =====================================================================
-- Нагрузочный seed для outboxlib (task-level retry)
--
-- Структура Task в JSONB (json.Marshal без тегов → PascalCase):
--   ID, PipelineID, HandlerName, Payload,
--   Done, Failed, Attempt, MaxAttempts
--   Backoff { Kind, Base, Max, Factor, JitterFrac },
--   NextAttemptAt, LastError
--
-- time.Duration сериализуется как int64 наносекунд:
--   5s  = 5000000000
--   30s = 30000000000
--   5m  = 300000000000
--   30m = 1800000000000
-- =====================================================================

\timing on

-- ---------------------------------------------------------------------
-- 0. (ОПЦИОНАЛЬНО) Очистка перед заливкой.
-- ---------------------------------------------------------------------
TRUNCATE pipelines;
TRUNCATE task_logs;

-- ---------------------------------------------------------------------
-- 1. 200 «свежих» pending — старый формат поведения, дефолтные задачи.
--    Каждая задача: Attempt=0, NextAttemptAt=now, exponential 5s..30m, 3 попытки.
-- ---------------------------------------------------------------------
INSERT INTO pipelines (id, state, stages, next_attempt_at, last_error,
                       locked_by, locked_until, created_at, updated_at)
SELECT
    p_id,
    'pending',
    jsonb_build_array(
        jsonb_build_object(
            'Number', 1,
            'Tasks', jsonb_build_array(
                jsonb_build_object(
                    'ID',            t1_id,
                    'PipelineID',    p_id,
                    'HandlerName',   'send-email',
                    'Payload',       jsonb_build_object('to', 'user' || g || '@example.com'),
                    'Done',          false,
                    'Failed',        false,
                    'Attempt',       0,
                    'MaxAttempts',   3,
                    'Backoff',       jsonb_build_object(
                                         'Kind','exponential','Base',5000000000,
                                         'Max',1800000000000,'Factor',2,'JitterFrac',0.25),
                    'NextAttemptAt', now(),
                    'LastError',     ''
                )
            )
        ),
        jsonb_build_object(
            'Number', 2,
            'Tasks', jsonb_build_array(
                jsonb_build_object(
                    'ID',            t2_id,
                    'PipelineID',    p_id,
                    'HandlerName',   'notify-slack',
                    'Payload',       jsonb_build_object('channel', '#alerts'),
                    'Done',          false,
                    'Failed',        false,
                    'Attempt',       0,
                    'MaxAttempts',   3,
                    'Backoff',       jsonb_build_object(
                                         'Kind','exponential','Base',5000000000,
                                         'Max',1800000000000,'Factor',2,'JitterFrac',0.25),
                    'NextAttemptAt', now(),
                    'LastError',     ''
                )
            )
        )
    ),
    now(), '',
    NULL, NULL,
    now(), now()
FROM (
    SELECT gen_random_uuid() AS p_id,
           gen_random_uuid() AS t1_id,
           gen_random_uuid() AS t2_id,
           g
    FROM generate_series(1, 200) AS g
) s;

-- ---------------------------------------------------------------------
-- 2. 100 retry — задача с разными backoff'ами, attempt 1..5.
--    next_attempt_at разбросан от -30s до +2h, поэтому часть уже готова,
--    часть подождёт.
-- ---------------------------------------------------------------------
INSERT INTO pipelines (id, state, stages, next_attempt_at, last_error,
                       locked_by, locked_until, created_at, updated_at)
SELECT
    p_id,
    'pending',
    jsonb_build_array(
        jsonb_build_object(
            'Number', 1,
            'Tasks', jsonb_build_array(
                jsonb_build_object(
                    'ID',            t_id,
                    'PipelineID',    p_id,
                    'HandlerName',   'flaky',
                    'Payload',       jsonb_build_object('n', g),
                    'Done',          false,
                    'Failed',        false,
                    'Attempt',       (g % 5) + 1,               -- 1..5
                    'MaxAttempts',   10,
                    'Backoff',       jsonb_build_object(
                                         'Kind','exponential','Base',5000000000,
                                         'Max',1800000000000,'Factor',2,'JitterFrac',0.25),
                    'NextAttemptAt', now() + ((g % 5) * interval '20 minutes')
                                            - interval '30 seconds',
                    'LastError',     'timeout: context deadline exceeded'
                )
            )
        )
    ),
    now() + ((g % 5) * interval '20 minutes') - interval '30 seconds',
    'flaky task pending retry',
    NULL, NULL,
    now() - interval '10 minutes', now()
FROM (
    SELECT gen_random_uuid() AS p_id,
           gen_random_uuid() AS t_id,
           g
    FROM generate_series(1, 100) AS g
) s;

-- ---------------------------------------------------------------------
-- 3. 50 running с АКТИВНЫМ lease — «соседний под работает».
--    ClaimDue их трогать не должен, пока locked_until > now().
-- ---------------------------------------------------------------------
INSERT INTO pipelines (id, state, stages, next_attempt_at, last_error,
                       locked_by, locked_until, created_at, updated_at)
SELECT
    p_id,
    'running',
    jsonb_build_array(
        jsonb_build_object(
            'Number', 1,
            'Tasks', jsonb_build_array(
                jsonb_build_object(
                    'ID',            t_id,
                    'PipelineID',    p_id,
                    'HandlerName',   'send-email',
                    'Payload',       jsonb_build_object('to', 'locked' || g || '@example.com'),
                    'Done',          false,
                    'Failed',        false,
                    'Attempt',       1,
                    'MaxAttempts',   3,
                    'Backoff',       jsonb_build_object(
                                         'Kind','exponential','Base',5000000000,
                                         'Max',1800000000000,'Factor',2,'JitterFrac',0.25),
                    'NextAttemptAt', now(),
                    'LastError',     ''
                )
            )
        )
    ),
    now(), '',
    'dead-pod-' || g,
    now() + interval '10 minutes',
    now(), now()
FROM (
    SELECT gen_random_uuid() AS p_id,
           gen_random_uuid() AS t_id,
           g
    FROM generate_series(1, 50) AS g
) s;

-- ---------------------------------------------------------------------
-- 4. 30 running с ИСТЁКШИМ lease — «сироты» после падения пода.
--    Должны быть перехвачены первым же ClaimDue.
-- ---------------------------------------------------------------------
INSERT INTO pipelines (id, state, stages, next_attempt_at, last_error,
                       locked_by, locked_until, created_at, updated_at)
SELECT
    p_id,
    'running',
    jsonb_build_array(
        jsonb_build_object(
            'Number', 1,
            'Tasks', jsonb_build_array(
                jsonb_build_object(
                    'ID',            t_id,
                    'PipelineID',    p_id,
                    'HandlerName',   'notify-slack',
                    'Payload',       jsonb_build_object('channel', '#orphan'),
                    'Done',          false,
                    'Failed',        false,
                    'Attempt',       1,
                    'MaxAttempts',   3,
                    'Backoff',       jsonb_build_object(
                                         'Kind','exponential','Base',5000000000,
                                         'Max',1800000000000,'Factor',2,'JitterFrac',0.25),
                    'NextAttemptAt', now(),
                    'LastError',     'lease expired'
                )
            )
        )
    ),
    now(), 'orphaned after pod crash',
    'dead-pod-' || g,
    now() - interval '5 minutes',
    now() - interval '1 hour', now()
FROM (
    SELECT gen_random_uuid() AS p_id,
           gen_random_uuid() AS t_id,
           g
    FROM generate_series(1, 30) AS g
) s;

-- ---------------------------------------------------------------------
-- 5. 100 complete — все задачи Done=true. Никогда не должны попасть в claim.
-- ---------------------------------------------------------------------
INSERT INTO pipelines (id, state, stages, next_attempt_at, last_error,
                       locked_by, locked_until, created_at, updated_at)
SELECT
    p_id,
    'complete',
    jsonb_build_array(
        jsonb_build_object(
            'Number', 1,
            'Tasks', jsonb_build_array(
                jsonb_build_object(
                    'ID',            t_id,
                    'PipelineID',    p_id,
                    'HandlerName',   'send-email',
                    'Payload',       jsonb_build_object('to', 'done' || g || '@example.com'),
                    'Done',          true,
                    'Failed',        false,
                    'Attempt',       1,
                    'MaxAttempts',   3,
                    'Backoff',       jsonb_build_object(
                                         'Kind','exponential','Base',5000000000,
                                         'Max',1800000000000,'Factor',2,'JitterFrac',0.25),
                    'NextAttemptAt', now() - interval '1 day',
                    'LastError',     ''
                )
            )
        )
    ),
    now() - interval '1 day', '',
    NULL, NULL,
    now() - interval '1 day', now() - interval '1 day'
FROM (
    SELECT gen_random_uuid() AS p_id,
           gen_random_uuid() AS t_id,
           g
    FROM generate_series(1, 100) AS g
) s;

-- ---------------------------------------------------------------------
-- 6. 20 failed — задача исчерпала попытки, терминально.
--    Не должна ретраиться.
-- ---------------------------------------------------------------------
INSERT INTO pipelines (id, state, stages, next_attempt_at, last_error,
                       locked_by, locked_until, created_at, updated_at)
SELECT
    p_id,
    'failed',
    jsonb_build_array(
        jsonb_build_object(
            'Number', 1,
            'Tasks', jsonb_build_array(
                jsonb_build_object(
                    'ID',            t_id,
                    'PipelineID',    p_id,
                    'HandlerName',   'send-email',
                    'Payload',       jsonb_build_object('to', 'dead' || g || '@example.com'),
                    'Done',          false,
                    'Failed',        true,
                    'Attempt',       3,
                    'MaxAttempts',   3,
                    'Backoff',       jsonb_build_object(
                                         'Kind','exponential','Base',5000000000,
                                         'Max',1800000000000,'Factor',2,'JitterFrac',0.25),
                    'NextAttemptAt', now() - interval '1 hour',
                    'LastError',     'connection refused'
                )
            )
        )
    ),
    now() - interval '1 hour',
    'task <uuid> (send-email) failed after 3 attempts: connection refused',
    NULL, NULL,
    now() - interval '1 day', now()
FROM (
    SELECT gen_random_uuid() AS p_id,
           gen_random_uuid() AS t_id,
           g
    FROM generate_series(1, 20) AS g
) s;

-- ---------------------------------------------------------------------
-- 7. 10 pending с БЕСКОНЕЧНЫМИ попытками — задача обязана доехать.
--    Backoff fixed 30s, Unlimited=true.
--    Ключевой кейс: даже после 100 попыток Failed никогда не станет.
-- ---------------------------------------------------------------------
INSERT INTO pipelines (id, state, stages, next_attempt_at, last_error,
                       locked_by, locked_until, created_at, updated_at)
SELECT
    p_id,
    'pending',
    jsonb_build_array(
        jsonb_build_object(
            'Number', 1,
            'Tasks', jsonb_build_array(
                jsonb_build_object(
                    'ID',            t_id,
                    'PipelineID',    p_id,
                    'HandlerName',   'send-email',
                    'Payload',       jsonb_build_object('to', 'stubborn' || g || '@example.com'),
                    'Done',          false,
                    'Failed',        false,
                    'Attempt',       5,
                    'MaxAttempts',   0,
                    'Backoff',       jsonb_build_object(
                                         'Kind','fixed','Base',30000000000,
                                         'Max',30000000000,'Factor',0,'JitterFrac',0),
                    'NextAttemptAt', now() - interval '10 seconds',
                    'LastError',     'SMTP 421: try again later'
                )
            )
        )
    ),
    now() - interval '10 seconds',
    'waiting for SMTP',
    NULL, NULL,
    now() - interval '3 hours', now()
FROM (
    SELECT gen_random_uuid() AS p_id,
           gen_random_uuid() AS t_id,
           g
    FROM generate_series(1, 10) AS g
) s;

-- ---------------------------------------------------------------------
-- 8. 10 pipeline'ов со СМЕШАННЫМИ backoff'ами в одном стейдже.
--    Задача A: fixed 5s. Задача B: exponential 5s..30m. Задача C: unlimited fixed 30s.
--    Проверяем, что pipeline.NextAttemptAt = min(tasks).
-- ---------------------------------------------------------------------
INSERT INTO pipelines (id, state, stages, next_attempt_at, last_error,
                       locked_by, locked_until, created_at, updated_at)
SELECT
    p_id,
    'pending',
    jsonb_build_array(
        jsonb_build_object(
            'Number', 1,
            'Tasks', jsonb_build_array(
                jsonb_build_object(
                    'ID',            ta_id,
                    'PipelineID',    p_id,
                    'HandlerName',   'fast',
                    'Payload',       NULL,
                    'Done',          false,
                    'Failed',        false,
                    'Attempt',       2,
                    'MaxAttempts',   5,
                    'Backoff',       jsonb_build_object(
                                         'Kind','fixed','Base',5000000000,
                                         'Max',5000000000,'Factor',0,'JitterFrac',0),
                    'NextAttemptAt', now() + interval '5 seconds',
                    'LastError',     'boom'
                ),
                jsonb_build_object(
                    'ID',            tb_id,
                    'PipelineID',    p_id,
                    'HandlerName',   'slow',
                    'Payload',       NULL,
                    'Done',          false,
                    'Failed',        false,
                    'Attempt',       2,
                    'MaxAttempts',   5,
                    'Backoff',       jsonb_build_object(
                                         'Kind','exponential','Base',5000000000,
                                         'Max',1800000000000,'Factor',2,'JitterFrac',0.25),
                    'NextAttemptAt', now() + interval '20 seconds',
                    'LastError',     'boom'
                ),
                jsonb_build_object(
                    'ID',            tc_id,
                    'PipelineID',    p_id,
                    'HandlerName',   'stubborn',
                    'Payload',       NULL,
                    'Done',          false,
                    'Failed',        false,
                    'Attempt',       10,
                    'MaxAttempts',   0,
                    'Backoff',       jsonb_build_object(
                                         'Kind','fixed','Base',30000000000,
                                         'Max',30000000000,'Factor',0,'JitterFrac',0),
                    'NextAttemptAt', now() + interval '30 seconds',
                    'LastError',     'still failing'
                )
            )
        )
    ),
    -- pipeline.NextAttemptAt = min(5s, 20s, 30s) = 5s
    now() + interval '5 seconds',
    'multiple tasks retrying',
    NULL, NULL,
    now() - interval '5 minutes', now()
FROM (
    SELECT gen_random_uuid() AS p_id,
           gen_random_uuid() AS ta_id,
           gen_random_uuid() AS tb_id,
           gen_random_uuid() AS tc_id,
           g
    FROM generate_series(1, 10) AS g
) s;

-- ---------------------------------------------------------------------
-- Итог
-- ---------------------------------------------------------------------
SELECT state, COUNT(*) AS cnt
FROM pipelines
GROUP BY state
ORDER BY state;

SELECT
    'total pipelines' AS metric, COUNT(*)::text AS value FROM pipelines
UNION ALL
SELECT 'tasks', COUNT(*)::text
FROM pipelines p,
     jsonb_array_elements(p.stages) s,
     jsonb_array_elements(s->'Tasks') t
UNION ALL
SELECT 'tasks Done', COUNT(*)::text
FROM pipelines p,
     jsonb_array_elements(p.stages) s,
     jsonb_array_elements(s->'Tasks') t
WHERE (t->>'Done')::bool = true
UNION ALL
SELECT 'tasks Failed', COUNT(*)::text
FROM pipelines p,
     jsonb_array_elements(p.stages) s,
     jsonb_array_elements(s->'Tasks') t
WHERE (t->>'Failed')::bool = true
UNION ALL
SELECT 'tasks Unlimited', COUNT(*)::text
FROM pipelines p,
     jsonb_array_elements(p.stages) s,
     jsonb_array_elements(s->'Tasks') t
WHERE (t->>'MaxAttempts')::int = 0;
