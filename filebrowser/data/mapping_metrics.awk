# функция для эскейпинга лейблов в prometheus
function escape_label(val) {
    gsub(/\\/, "\\\\", val)
    gsub(/"/, "\\\"", val)
    gsub(/\n/, "\\n", val)
    gsub(/\t/, "\\t", val)
    return val
}

function escape_json(str, out) {
    gsub(/\\/, "\\\\", str)
    gsub(/"/, "\\\"", str)
    return str
}

function to_json(obj, json, key) {
    json = "{"
    first = 1
    for (key in obj) {
        if (!first) json = json ", "
        json = json "\"" escape_json(key) "\": \"" escape_json(obj[key]) "\""
        first = 0
    }
    json = json "}"
    return json
}

# поиск Job ID в логе formit
# 2025-06-19 13:42:10,258 MSK <MappingCompiler-pool-4-thread-4> INFO: [MPSVCCMN_10083] The Mapping Service Module submitted a job to the Integration Service.  Job ID : [DVO1PUz6EfCfo_Iq4JhcNg]
/job to the Integration Service.  Job ID : / {
    # извлекаем первое имя в квадратных скобках
    if (match($0, /job to the Integration Service.  Job ID : \[([^\]]+)\]/, arr)) {
        job_id = arr[1]
    }
}

# поиск mapping name и application name
# 2025-06-19 13:42:10,258 MSK <MappingCompiler-pool-4-thread-4> INFO: [MPSVCCMN_10081] Mapping service is running [m_oracle_full_reload_with_create_schema] deployed in [app_brd_ngtsmart_rdfsmart]
/Mapping service is running / {
    # извлекаем второе имя в квадратных скобках
    if (match($0, /deployed in \[([^\]]+)\]/, arr)) {
        app_name = arr[1]
    }
}

# Creating Session [jtx_shell_MAPPING_m_sleep] in process [3027854] on platform [Linux] (architecture [amd64]), product version [10.5.3], build version [149], build date [2022-10-26 19:00:27,000 MSK].
/Creating Session/ {
    # извлекаем первое имя в квадратных скобках
    if (match($0, /Creating Session \[([^\]]+)\] in process/, arr)) {
        mapping_name = arr[1]
    }
}

# поиск LDTM time
# 2025-06-19 13:42:10,987 MSK <MappingCompiler-pool-4-thread-4> INFO: [LDTM_0074] Total time to create the LDTM: 696 ms
/Total time to create the LDTM: / {
    # извлекаем первое имя в квадратных скобках
    if (match($0, /Total time to create the LDTM: ([0-9]+) ms/, arr)) {
        ldtm_duration_sec = arr[1] /1000
    }
}

# поиск dis в логе
# 2025-06-19 13:42:10,272 MSK <MappingCompiler-pool-4-thread-4> INFO: [DS_10178] The job is running on Integration Service [dis_etl_brd_prod_01]. The Launch Job Options for the service is set to: [In separate local processes (OUT_OF_PROCESS)].
/The job is running on Integration Service / {
    # вырезаем подстроку начиная с pos
    if (match($0, /The job is running on Integration Service \[([^\]]+)\]/, arr)) {
        dis_name = arr[1]
    }
}

# поиск DTMProcess_{dmt_process}
# 2025-06-19 13:42:11,009 MSK <APP_NativeBatchDTM-pool-3-thread-15010> INFO: [JDTM_10009] The Integration Service is dispatching the job to the DTM process [DTMProcess_15644].
/The Integration Service is dispatching the job to the DTM process / {
    # вырезаем подстроку начиная с pos
    if (match($0, /The Integration Service is dispatching the job to the DTM process \[DTMProcess_([0-9]+)\]/, arr)) {
        dmt_process = arr[1]
    }
}

# поиск параметров запуска маппинга
# 2025-06-19 13:42:10,292 MSK <MappingCompiler-pool-4-thread-4> INFO: [LDTMPARAM_0011] The Integration Service uses the override value [con_ois_usoi_SPB99ORA01_oracle_h1] for the mapping parameter [src_conn].
# 2025-06-19 13:42:10,292 MSK <MappingCompiler-pool-4-thread-4> INFO: [LDTMPARAM_0011] The Integration Service uses the override value [RDFSMART] for the mapping parameter [src_schema].
# 2025-06-19 13:42:10,292 MSK <MappingCompiler-pool-4-thread-4> 
/uses the (override )?value \[.*\] for the (mapping|system) parameter \[.*\]/ {
    match($0, /value \[([^\]]+)\] for .*parameter \[([^\]]+)\]/, arr)
    if (arr[1] != "" && arr[2] != "")
        mapping_params[arr[2]] = arr[1]
}

# Поиск информации об схеме и таблице источника
# 2025-06-19 13:42:10,445 MSK <MappingCompiler-pool-4-thread-4> INFO: [LDTMEXP_0024] Starting synchronization for transformation [src_ora] .
# 2025-06-19 13:42:10,567 MSK <MappingCompiler-pool-4-thread-4> INFO: [REL_10218] Getting metadata for schema (RDFSMART), table (WELL_OP_INJ) by calling getTables method.
/Getting metadata for schema/ {
    # вырезаем подстроку начиная с pos
    if (match($0, /Getting metadata for schema \(([^)]+)\), table \(([^)]+)\) by calling getTables method/, arr)) {
        if (arr[1] != "" && arr[2] != "") {
            source_schema = arr[1]
            source_table = arr[2]
        }
    }
}

# поиск process id и node в логе
# 2025-06-19 13:42:11,035 MSK <APP_140013424842496> INFO: [DISP_20305] The [Master] DTM with process id [809819] is running on node [node_spb99pkl_dpls2].
/DTM with process id / {
    if (match($0, /DTM with process id \[([^\]]+)\]/, arr)) {
        pid = arr[1]
    }

    if (match($0, /is running on node \[([^\]]+)\]/, arr)) {
        node = arr[1]
    }
}

# получаю информацию о создаваемой таблице
/Starting the recreation operation for target table/ {
    if (match($0, /\["([^"]+)"\."([^"]+)"\]/, arr)) {
        target_schema = arr[1]
        target_table = arr[2]
        target_schema_strategy = "recreate"
    }
}

# получаю sql стейт на drop таблицы
/DDL Statement for \[drop table\]/ {
    if (match($0, /DDL Statement for \[drop table\]: \[([^\]]+)\]/, arr)) {
        target_ddl_drop = arr[1]
    }
}

# получаю sql стейт на create таблицы
/DDL Statement for \[create table\]/ {
    if (match($0, /DDL Statement for \[create table\]: \[([^\]]+)\]/, arr)) {
        target_ddl_create = arr[1]

        # достаю из create table каждое поле отдельно
        if (target_ddl_create != "") {
            rest = target_ddl_create
            gsub(/\n/, " ", rest)
            gsub(/\r/, " ", rest)

            if (match(rest, /\((.*)\)[[:space:]]*$/, arr)) {
                cols_block = arr[1]

                while (match(cols_block, /"[^"]+"\s+[A-Z]+[A-Z0-9_(), ]*/)) {
                    field = substr(cols_block, RSTART, RLENGTH)

                    if (match(field, /"([^"]+)"\s+(.+)/, fm)) {
                        col_name = fm[1]
                        data_type = fm[2]
                        # удаляю запятую если она есть
                        sub(/, $/, "", data_type)
                        # print "col_name=" col_name
                        # print "data_type=" data_type

                        mapping_columns[col_name] = data_type
                    }
                    cols_block = substr(cols_block, RSTART + RLENGTH)
                }
            }
        }
    }
}


# получаю sql стейт на select таблицы
/SQ instance \[.*\] SQL Query \[SELECT/ {
    if (match($0, /SQ instance \[([^\]]+)\] SQL Query \[SELECT ([^\]]+) FROM ([^\]]+)\]/, arr)) {
        source_transform_name = arr[1]
        source_transform_select = arr[2]
        source_transform_from = arr[3]

        if (source_transform_select != "") {
            rest = source_transform_select
            # удаляю название (вернее путь) к таблице из каждого столбца
            gsub(source_transform_from ".", "", rest)
            gsub(/\n/, " ", rest)
            gsub(/\r/, " ", rest)

            while (match(rest, /[^\\"]+/)) {
                field = substr(rest, RSTART, RLENGTH)
                source_transform_columns[field] = field
                # +3 символа для пропуска запятой
                rest = substr(rest, RSTART + RLENGTH + 3)
            }
        }
    }
}

# поиск oracle SID и пользователя в connection источника
# 2025-06-19 13:42:11,195 MSK <TASK_140013056493312-READER_1_1_1> INFO: [DBG_21438] Reader: Source is [SPB99ORADG01], user [klad_pr2sb]
# 2025-06-19 00:36:55,480 MSK <TASK_140599378278144-READER_1_1_1> INFO: [DBG_21438] Reader: Source is [], user [geobd_upstr_int]
/Reader: Source is / {
    # извлекаем первое имя в квадратных скобках
    if (match($0, /Reader: Source is \[([^\]]+)\]/, arr)) {
        source_reader_name = arr[1]
    }

    # извлекаем второе имя в квадратных скобках
    if (match($0, /, user \[([^\]]+)\]/, arr)) {
        source_user_name = arr[1]
    }
}

# поиск source database connection
# 2025-06-19 13:42:11,196 MSK <TASK_140013056493312-READER_1_1_1> INFO: [BLKR_16051] Source database connection [con_ois_usoi_SPB99ORA01_oracle_h1] code page: [UTF-8 encoding of Unicode]
/Source database connection / {
    # извлекаем первое имя в квадратных скобках
    if (match($0, /Source database connection \[([^\]]+)\]/, arr)) {
        source_database_connection = arr[1]
    }
}

# поиск target database connection
# 2025-06-19 13:42:11,366 MSK <TASK_140013056493312-WRITER_1_*_1> INFO: [WRT_8221] Target database connection [con__dp_pg_brd__edw_brd__jdbc__h1] code page: [UTF-8 encoding of Unicode]
/Target database connection / {
    # извлекаем первое имя в квадратных скобках
    if (match($0, /Target database connection \[([^\]]+)\]/, arr)) {
        target_database_connection = arr[1]
    }
}


# поиск oracle SID и пользователя в connection назначения
# 2025-06-19 13:42:11,279 MSK <TASK_140013056493312-WRITER_1_*_1> INFO: [WRT_8147] Writer: Target is database [], user [dp_w_p_tin_pls], bulk mode [OFF]
# 2025-06-19 00:36:55,617 MSK <TASK_140599369885440-WRITER_1_*_1> INFO: [WRT_8147] Writer: Target is database [], user [dp_w_p_tin_pls], bulk mode [OFF]
/Writer: Target is / {
    # извлекаем первое имя в квадратных скобках
    if (match($0, /Writer: Target is database \[([^\]]+)\]/, arr)) {
        target_writer_name = arr[1]
    }

    # извлекаем второе имя в квадратных скобках
    if (match($0, /, user \[([^\]]+)\]/, arr)) {
        target_user_name = arr[1]
    }

    # извлекаем bulk mode
    if (match($0, /bulk mode \[([^\]]+)\]/, arr)) {
        bulk_mode = arr[1]
    }
}

# поиск показателей total buffer size
# 2025-06-19 13:42:11,193 MSK <TASK_140013064886016-MAPPING> INFO: [TM_6660] Total Buffer Pool size is 12100824 bytes and Block size is 1344536 bytes.
/Total Buffer Pool size is / {
    # извлекаем первое имя в квадратных скобках
    if (match($0, /Total Buffer Pool size is ([0-9]+) bytes/, arr)) {
        total_buffer_size = arr[1]
    }

    # извлекаем второе имя в квадратных скобках
    if (match($0, /and Block size is ([0-9]+) bytes/, arr)) {
        block_size = arr[1]
    }
}

# Поиск даты-времени в начале строки: YYYY-MM-DD HH:MM:SS,мс
/^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2},[0-9]{3}/ {
    # Вытаскиваем дату-время (первые 23 символа: "2025-06-19 13:42:10,599")
    timestamp = substr($0, 1, 23)

    # Первая найденная метка времени
    if (start == "") start = timestamp

    # Последняя найденная метка времени постоянно обновляется
    end = timestamp

    # парсим даты
    # Для парсинга используем split и substr
    # Формат: "2025-06-19 13:42:10,599"
    split(start, s_parts, /[- :,]/)
    split(end, e_parts, /[- :,]/)

    start_sec = mktime(s_parts[1]" "s_parts[2]" "s_parts[3]" "s_parts[4]" "s_parts[5]" "s_parts[6])
    end_sec   = mktime(e_parts[1]" "e_parts[2]" "e_parts[3]" "e_parts[4]" "e_parts[5]" "e_parts[6])

    # миллисекунды
    start_ms = s_parts[7]
    end_ms = e_parts[7]

    # общая разница в секундах с миллисекундами
    duration_sec = (end_sec - start_sec) + (end_ms - start_ms)/1000
}

# ============================================================================
# ВЫСОКОПРИОРИТЕТНЫЕ МЕТРИКИ
# ============================================================================

# поиск строк прочитанных из источника (BLKR_16019)
# 2026-02-02 14:24:13,562 MSK ... INFO: [BLKR_16019] Read [2] rows, read [0] error rows for source table [my_super_csv] instance name [Read_fake_01]
/\[BLKR_16019\] Read / {
    if (match($0, /Read \[([0-9]+)\] rows/, arr)) {
        source_rows_read = arr[1]
    }
    if (match($0, /read \[([0-9]+)\] error rows/, arr)) {
        source_error_rows = arr[1]
    }
}

# поиск ошибок трансформации (TM_6018)
# 2026-02-02 14:24:13,577 MSK ... INFO: [TM_6018] The session completed with [0] row transformation errors.
/\[TM_6018\].*row transformation errors/ {
    if (match($0, /completed with \[([0-9]+)\] row transformation errors/, arr)) {
        row_transformation_errors = arr[1]
    }
}

# breakdown по стадиям reader/transformation/writer (PETL_24031)
# Completed the [reader] stage of the partition point [src_ora].  The run time is [56.146225] seconds, which is [5.843455] percent of the load order group run time.
# Completed the [reader] stage of the partition point [src]. The run time is not sufficient to report any statistics.
/Completed the \[reader\] stage/ {
    if (match($0, /run time is \[([0-9.]+)\] seconds/, arr)) {
        reader_duration_sec = arr[1]
    }
    if (match($0, /which is \[([0-9.]+)\] percent/, arr)) {
        reader_percent = arr[1]
    }
}

/Completed the \[transformation\] stage/ {
    if (match($0, /run time is \[([0-9.]+)\] seconds/, arr)) {
        transform_duration_sec = arr[1]
    }
    if (match($0, /which is \[([0-9.]+)\] percent/, arr)) {
        transform_percent = arr[1]
    }
}

/Completed the \[writer\] stage/ {
    if (match($0, /run time is \[([0-9.]+)\] seconds/, arr)) {
        writer_duration_sec = arr[1]
    }
    if (match($0, /which is \[([0-9.]+)\] percent/, arr)) {
        writer_percent = arr[1]
    }
}

# память DIS — общая и доступная (CMN_65162 / CMN_65166)
# [CMN_65162] The total Integration Service memory is set to [1073741824] bytes. The maximum memory size is set to [200000000] bytes.
/\[CMN_65162\]/ {
    if (match($0, /total Integration Service memory is set to \[([0-9]+)\]/, arr)) {
        total_service_memory = arr[1]
    }
    if (match($0, /maximum memory size is set to \[([0-9]+)\]/, arr)) {
        max_memory_size = arr[1]
    }
}

# [CMN_65166] The available Integration Service memory is [1047995920] bytes.
/\[CMN_65166\]/ {
    if (match($0, /available Integration Service memory is \[([0-9]+)\]/, arr)) {
        available_memory_after = arr[1]
    }
}

# подсчёт количества commit point (WRT_8161)
# TARGET BASED COMMIT POINT  Mon Feb 02 21:05:35 2026
/\[WRT_8161\]/ {
    commit_point_count++
}

# ============================================================================
# СРЕДНЕПРИОРИТЕТНЫЕ МЕТРИКИ
# ============================================================================

# thread pool size (TM_6983)
# [TM_6983] Size of the thread pool that the DTM uses to allocate CPU resources: [16].
/\[TM_6983\]/ {
    if (match($0, /allocate CPU resources: \[([0-9]+)\]/, arr)) {
        thread_pool_size = arr[1]
    }
}

# время формирования partition group (TM_6722)
# [TM_6722] Finished [Partition Group Formation].  It took [0.000354052] seconds.
/\[TM_6722\].*Partition Group Formation/ {
    if (match($0, /It took \[([0-9.]+)\] seconds/, arr)) {
        partition_formation_sec = arr[1]
    }
}

# статус pre-session (PETL_24027 — success, PETL_24028 — failure)
/Pre-session task completed/ {
    if (match($0, /completed successfully/)) {
        pre_session_status = "success"
    } else if (match($0, /completed with failure/)) {
        pre_session_status = "failure"
    }
}

# статус post-session (PETL_24029 — success, PETL_24028 — failure)
/Post-session task completed/ {
    if (match($0, /completed successfully/)) {
        post_session_status = "success"
    } else if (match($0, /completed with failure/)) {
        post_session_status = "failure"
    }
}

# финальный статус сессии (PETL_24012 — success, PETL_24013 — failure)
/Session run completed/ {
    if (match($0, /completed successfully/)) {
        session_status = "success"
    } else if (match($0, /completed with failure/)) {
        session_status = "failure"
    }
}

# ============================================================================

# вытаскиваю показатели Requested, Applied, Rejected, Affected
/Inserted rows - Requested:/ {
    if (match($0, /Requested: *([0-9]+)/, r)) requested = r[1]
    if (match($0, /Applied: *([0-9]+)/, a)) applied = a[1]
    if (match($0, /Rejected: *([0-9]+)/, b)) rejected = b[1]
    if (match($0, /Affected: *([0-9]+)/, f)) affected = f[1]
}

# попытка найти некорректно завершенные таски (вероятно это kill -9)
# 2025-06-19 13:49:28,363 MSK <TASK_140013073278720-WRITER_1_*_1> SEVERE: [_0] com.informatica.dtm.transport.DTFUncheckedException: [DtmDTF_0001] Data Transport Error, Origin :[Pipe :: receiveMessage].
/Data Transport Error/ {
    # устанавливаем признак ошибки
    if (match($0, /Pipe :: receiveMessage/, arr)) {
        error_received = 1
        error_code = "DtmDTF_0001"
        error_text = $0
    }
}

# попытка найти ошибку в работе сессии с базой данных (CMN_1021)
# CMN_1021 [
# FnName: INFASQLExecute -- execute(). SQLException: [java.sql.BatchUpdateException: [[informatica][PostgreSQL JDBC Driver][PostgreSQL]значение не умещается в тип character varying(100).  The entire batch operation was rolled back.]]
# Function [INFASQLGetDiagRecW] failed in adapter [/data1/formit/v131/plugins/dynamic/infajdbcruntime/libinfajdbc.so] with error code [100].]
/CMN_([0-9]+) \[/ {
    error_received = 1
    error_code = $1
    error_text = ""
    error_cmn_found = 1

    if (getline) {
        # выбираю текст ошибки из следующей строки после CMN_\d+
        error_text = $0
    }
}

# если найдена ошибка то пытаюсь уточнить текст через Database Error
/Database Error: ([^\]]+)/ {
    if (error_received == 1) {
        error_text = $0
    }
}

# Попытка найти ошибку в java runtime
/Caused by: java.lang.RuntimeException: (.*)/ {
    error_received = 1
    error_code = "java_runtime"
    if (match($0, /Caused by: java.lang.RuntimeException: ([^\]]+)/, arr)) {
        error_text = arr[1]
    }
}

# поиск SEVERE ошибок в логе (SQL Error, Prepare failed и т.д.)
# 2026-02-10 02:26:15,353 MSK <TASK_140101413721856-READER_1_1_1> SEVERE: [RR_4035] SQL Error [
/SEVERE:/ {
    if (severe_found != 1) {
        severe_found = 1
        error_received = 1
        error_cmn_found = 0
        if (match($0, /SEVERE: \[([^\]]+)\]/, arr)) {
            error_code = arr[1]
        }
        error_text = $0

        # Проверка на многострочное сообщение в квадратных скобках:
        # если строка заканчивается на [, то читаем следующие строки до закрывающей ]
        tmp = $0
        sub(/[[:space:]]+$/, "", tmp)
        if (substr(tmp, length(tmp), 1) == "[") {
            bracket_depth = 1
            while (bracket_depth > 0 && (getline line) > 0) {
                error_text = error_text "\n" line
                for (i = 1; i <= length(line); i++) {
                    c = substr(line, i, 1)
                    if (c == "[") bracket_depth++
                    else if (c == "]") bracket_depth--
                    if (bracket_depth <= 0) break
                }
            }
        }
    }
}

# уточнение текста ошибки при наличии SQLException
# FnName: INFASQLPrepare -- prepare(). SQLException: [java.sql.SQLSyntaxErrorException: [[informatica][PostgreSQL JDBC Driver][PostgreSQL]отношение "well_info.v_r_field_station_type" не существует. ]]
/SQLException:/ {
    if (error_received == 1 && severe_found != 1) {
        if (match($0, /SQLException: (.+)/, arr)) {
            error_text = arr[1]
        }
    }
}

# Появление этой надписи считаю признаком завершения маппинга (вероятно маппинг считает, что он завершился корректно)
/\[EdtmExec_00006\] LDTM: Mapping execution done/ {
    execution_done = 1
}

# выводим в prometheus метрику
END {
    # Это исключение! если заматчился текст вот этот, то не считаю ошибкой
    if (match(error_text, /RELSDK Event Using Array Inserts/, arr)) {
        error_received = 0
        error_code = ""
        error_text = ""
        error_cmn_found = 0
    }

    # экранируем двойные кавычки если они есть
    gsub(/"/, "\\\"", mapping_name)
    gsub(/"/, "\\\"", app_name)
    gsub(/"/, "\\\"", job_id)
    gsub(/"/, "\\\"", dis_name)

    # записываю первый лейбл
    if (mapping_name != "") {
        output = output "mapping_name=\"" mapping_name "\""
    }
    labels["log"] = escape_label(ENVIRON["p_log_file"])
    labels["node"] = escape_label(node)
    labels["dis_name"] = escape_label(dis_name)
    labels["app_name"] = escape_label(app_name)
    labels["job_id"] = escape_label(job_id)
    labels["pid"] = escape_label(pid)

    labels["execution_done"] = escape_label(execution_done)
    labels["source_database_connection"] = escape_label(source_database_connection)
    labels["source_reader_name"] = escape_label(source_reader_name)
    labels["source_user_name"] = escape_label(source_user_name)
    labels["source_schema"] = escape_label(source_schema)
    labels["source_table"] = escape_label(source_table)

    labels["target_database_connection"] = escape_label(target_database_connection)
    labels["target_writer_name"] = escape_label(target_writer_name)
    labels["target_user_name"] = escape_label(target_user_name)
    labels["target_schema"] = escape_label(target_schema)
    labels["target_table"] = escape_label(target_table)
    labels["target_schema_strategy"] = escape_label(target_schema_strategy)

    labels["bulk_mode"] = escape_label(bulk_mode)
    labels["session_status"] = escape_label(session_status)
    labels["pre_session_status"] = escape_label(pre_session_status)
    labels["post_session_status"] = escape_label(post_session_status)

    # добавляю информацию об исходных колонках
    # labels["target_ddl_drop"] = escape_label(target_ddl_drop)
    # labels["target_ddl_create"] = escape_label(target_ddl_create)

    # добавляю информацию о колонках из create table
    # labels["target_columns"] = escape_label(to_json(mapping_columns))

    # добавляю параметры запуска маппинга
    # labels["mapping_params"] = escape_label(to_json(mapping_params))

    # пишу остальные лейблы
    for (key in labels) {
        if (labels[key] != "") {
            output = output ", " key "=\"" labels[key] "\""
        }
    }

    # Признак того, что маппинг завешен
    if (execution_done != "") {
        print "mapping_execution_done{" output "} " 1
    } else {
        print "mapping_execution_done{" output "} " 0
    }

    if (dmt_process != "") {
        print "mapping_dmt_process{" output "} " dmt_process
    }
    if (total_buffer_size != "") {
        print "mapping_total_buffer_size{" output "} " total_buffer_size
    }
    if (block_size != "") {
        print "mapping_block_size{" output "} " block_size
    }
    if (start_sec != "") {
        print "mapping_start_sec{" output "} " start_sec
    }
    if (end_sec != "") {
        print "mapping_end_sec{" output "} " end_sec
    }
    if (duration_sec != "") {
        print "mapping_duration_sec{" output "} " duration_sec
    }
    if (requested != "") {
        print "mapping_requested{" output "} " requested
    }
    if (applied != "") {
        print "mapping_applied{" output "} " applied
    }
    if (rejected != "") {
        print "mapping_rejected{" output "} " rejected
    }
    if (affected != "") {
        print "mapping_affected{" output "} " affected
    }
    if (ldtm_duration_sec != "") {
        print "mapping_ldtm_duration_sec{" output "} " ldtm_duration_sec
    }

    # высокоприоритетные метрики
    if (source_rows_read != "") {
        print "mapping_source_rows_read{" output "} " source_rows_read
    }
    if (source_error_rows != "") {
        print "mapping_source_error_rows{" output "} " source_error_rows
    }
    if (row_transformation_errors != "") {
        print "mapping_row_transformation_errors{" output "} " row_transformation_errors
    }
    if (reader_duration_sec != "") {
        print "mapping_reader_duration_sec{" output "} " reader_duration_sec
    }
    if (reader_percent != "") {
        print "mapping_reader_percent{" output "} " reader_percent
    }
    if (transform_duration_sec != "") {
        print "mapping_transform_duration_sec{" output "} " transform_duration_sec
    }
    if (transform_percent != "") {
        print "mapping_transform_percent{" output "} " transform_percent
    }
    if (writer_duration_sec != "") {
        print "mapping_writer_duration_sec{" output "} " writer_duration_sec
    }
    if (writer_percent != "") {
        print "mapping_writer_percent{" output "} " writer_percent
    }
    if (total_service_memory != "") {
        print "mapping_total_service_memory_bytes{" output "} " total_service_memory
    }
    if (max_memory_size != "") {
        print "mapping_max_memory_size_bytes{" output "} " max_memory_size
    }
    if (available_memory_after != "") {
        print "mapping_available_memory_after_bytes{" output "} " available_memory_after
    }
    if (commit_point_count != "") {
        print "mapping_commit_point_count{" output "} " commit_point_count
    }

    # среднеприоритетные метрики
    if (thread_pool_size != "") {
        print "mapping_thread_pool_size{" output "} " thread_pool_size
    }
    if (partition_formation_sec != "") {
        print "mapping_partition_formation_sec{" output "} " partition_formation_sec
    }

    if (error_received == 1) {
        print "mapping_error{" output ", " "error_code=\"" escape_label(error_code) "\", error_text=\"" escape_label(error_text) "\"" "} " error_received
    }
}
