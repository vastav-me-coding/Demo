
C:\Users\A0885080\AppData\Roaming\Python\Python312\site-packages\urllib3\connectionpool.py:1097: InsecureRequestWarning: Unverified HTTPS request is being made to host 'secretsmanager.us-east-1.amazonaws.com'. Adding certificate verification is strongly advised. See: https://urllib3.readthedocs.io/en/latest/advanced-usage.html#tls-warnings
  warnings.warn(
C:\Users\A0885080\AppData\Roaming\Python\Python312\site-packages\urllib3\connectionpool.py:1097: InsecureRequestWarning: Unverified HTTPS request is being made to host 'secretsmanager.us-east-1.amazonaws.com'. Adding certificate verification is strongly advised. See: https://urllib3.readthedocs.io/en/latest/advanced-usage.html#tls-warnings
  warnings.warn(
C:\Users\A0885080\AppData\Roaming\Python\Python312\site-packages\urllib3\connectionpool.py:1097: InsecureRequestWarning: Unverified HTTPS request is being made to host 'secretsmanager.us-east-1.amazonaws.com'. Adding certificate verification is strongly advised. See: https://urllib3.readthedocs.io/en/latest/advanced-usage.html#tls-warnings
  warnings.warn(
[DEBUG] Starting consume_lambda at 2025-12-05 12:32:14.940116
[DEBUG] Raw config input: {"source_carrier_id":"A1FS102C","source_system":"as400_affprd"}
[DEBUG] Normalized config_dicts: [{'source_carrier_id': 'A1FS102C', 'source_system': 'as400_affprd'}]

[DEBUG] Processing config: {'source_carrier_id': 'A1FS102C', 'source_system': 'as400_affprd'}
[DEBUG] source_system=as400_affprd, alloc_id=A1FS102C
[DEBUG] AS400 lookup result for alloc_id A1FS102C: {'carrier_id': 'CNA', 'carrier_name': 'Continental Casualty Co. (CNA)', 'participant_percentage': '100', 'coverage_code': 'A1FS102C'}
[DEBUG] Source System not found → inserting new record for as400_affprd
53ce9c5a-e34a-55a9-b988-dfdd8054aa4f df_source_system_id
[ERROR] SQLAlchemyError encountered: (pymysql.err.IntegrityError) (1062, "Duplicate entry '53' for key 'source_system.PRIMARY'")
[SQL: INSERT INTO source_system (df_source_system_id, source_system, created_at, modified_at) VALUES (%(df_source_system_id)s, %(source_system)s, %(created_at)s, %(modified_at)s)]
[parameters: {'df_source_system_id': '53ce9c5a-e34a-55a9-b988-dfdd8054aa4f', 'source_system': 'as400_affprd', 'created_at': datetime.datetime(2025, 12, 5, 12, 32, 18, 799321), 'modified_at': datetime.datetime(2025, 12, 5, 12, 32, 18, 799321)}]
(Background on this error at: https://sqlalche.me/e/20/gkpj)
Traceback (most recent call last):
  File "C:\Users\A0885080\AppData\Roaming\Python\Python312\site-packages\sqlalchemy\engine\base.py", line 1963, in _exec_single_context
    self.dialect.do_execute(
  File "C:\Users\A0885080\AppData\Roaming\Python\Python312\site-packages\sqlalchemy\engine\default.py", line 943, in do_execute
    cursor.execute(statement, parameters)
  File "C:\Users\A0885080\AppData\Roaming\Python\Python312\site-packages\pymysql\cursors.py", line 153, in execute
    result = self._query(query)
             ^^^^^^^^^^^^^^^^^^
  File "C:\Users\A0885080\AppData\Roaming\Python\Python312\site-packages\pymysql\cursors.py", line 322, in _query
    conn.query(q)
  File "C:\Users\A0885080\AppData\Roaming\Python\Python312\site-packages\pymysql\connections.py", line 563, in query
    self._affected_rows = self._read_query_result(unbuffered=unbuffered)
                          ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "C:\Users\A0885080\AppData\Roaming\Python\Python312\site-packages\pymysql\connections.py", line 825, in _read_query_result
    result.read()
  File "C:\Users\A0885080\AppData\Roaming\Python\Python312\site-packages\pymysql\connections.py", line 1199, in read
    first_packet = self.connection._read_packet()
                   ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "C:\Users\A0885080\AppData\Roaming\Python\Python312\site-packages\pymysql\connections.py", line 775, in _read_packet
    packet.raise_for_error()
  File "C:\Users\A0885080\AppData\Roaming\Python\Python312\site-packages\pymysql\protocol.py", line 219, in raise_for_error
    err.raise_mysql_exception(self._data)
  File "C:\Users\A0885080\AppData\Roaming\Python\Python312\site-packages\pymysql\err.py", line 150, in raise_mysql_exception
    raise errorclass(errno, errval)
pymysql.err.IntegrityError: (1062, "Duplicate entry '53' for key 'source_system.PRIMARY'")

The above exception was the direct cause of the following exception:

Traceback (most recent call last):
  File "c:\Users\A0885080\Downloads\Fixed Repo Git\df_sqs_as400_aff_kkins_coveragecarrierallocationconsumer\src\handler.py", line 377, in <module>
    handle({'Records': [{'body': '{"source_carrier_id":"A1FS102C","source_system":"as400_affprd"}'}]}, None)
  File "c:\Users\A0885080\Downloads\Fixed Repo Git\df_sqs_as400_aff_kkins_coveragecarrierallocationconsumer\src\handler.py", line 372, in handle
    asyncio.run(consume_lambda(config=payload))
  File "C:\Program Files\Python312\Lib\asyncio\runners.py", line 194, in run
    return runner.run(main)
           ^^^^^^^^^^^^^^^^
  File "C:\Program Files\Python312\Lib\asyncio\runners.py", line 118, in run
    return self._loop.run_until_complete(task)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "C:\Program Files\Python312\Lib\asyncio\base_events.py", line 685, in run_until_complete
    return future.result()
           ^^^^^^^^^^^^^^^
  File "c:\Users\A0885080\Downloads\Fixed Repo Git\df_sqs_as400_aff_kkins_coveragecarrierallocationconsumer\src\handler.py", line 361, in consume_lambda
    raise e
  File "c:\Users\A0885080\Downloads\Fixed Repo Git\df_sqs_as400_aff_kkins_coveragecarrierallocationconsumer\src\handler.py", line 231, in consume_lambda
    session.commit()
  File "C:\Users\A0885080\AppData\Roaming\Python\Python312\site-packages\sqlalchemy\orm\session.py", line 2032, in commit
    trans.commit(_to_root=True)
  File "<string>", line 2, in commit
  File "C:\Users\A0885080\AppData\Roaming\Python\Python312\site-packages\sqlalchemy\orm\state_changes.py", line 139, in _go
    ret_value = fn(self, *arg, **kw)
                ^^^^^^^^^^^^^^^^^^^^
  File "C:\Users\A0885080\AppData\Roaming\Python\Python312\site-packages\sqlalchemy\orm\session.py", line 1313, in commit
    self._prepare_impl()
  File "<string>", line 2, in _prepare_impl
  File "C:\Users\A0885080\AppData\Roaming\Python\Python312\site-packages\sqlalchemy\orm\state_changes.py", line 139, in _go
    ret_value = fn(self, *arg, **kw)
                ^^^^^^^^^^^^^^^^^^^^
  File "C:\Users\A0885080\AppData\Roaming\Python\Python312\site-packages\sqlalchemy\orm\session.py", line 1288, in _prepare_impl
    self.session.flush()
  File "C:\Users\A0885080\AppData\Roaming\Python\Python312\site-packages\sqlalchemy\orm\session.py", line 4345, in flush
    self._flush(objects)
  File "C:\Users\A0885080\AppData\Roaming\Python\Python312\site-packages\sqlalchemy\orm\session.py", line 4480, in _flush
    with util.safe_reraise():
  File "C:\Users\A0885080\AppData\Roaming\Python\Python312\site-packages\sqlalchemy\util\langhelpers.py", line 224, in __exit__
    raise exc_value.with_traceback(exc_tb)
  File "C:\Users\A0885080\AppData\Roaming\Python\Python312\site-packages\sqlalchemy\orm\session.py", line 4441, in _flush
    flush_context.execute()
  File "C:\Users\A0885080\AppData\Roaming\Python\Python312\site-packages\sqlalchemy\orm\unitofwork.py", line 466, in execute
    rec.execute(self)
  File "C:\Users\A0885080\AppData\Roaming\Python\Python312\site-packages\sqlalchemy\orm\unitofwork.py", line 642, in execute
    util.preloaded.orm_persistence.save_obj(
  File "C:\Users\A0885080\AppData\Roaming\Python\Python312\site-packages\sqlalchemy\orm\persistence.py", line 93, in save_obj
    _emit_insert_statements(
  File "C:\Users\A0885080\AppData\Roaming\Python\Python312\site-packages\sqlalchemy\orm\persistence.py", line 1048, in _emit_insert_statements
    result = connection.execute(
             ^^^^^^^^^^^^^^^^^^^
  File "C:\Users\A0885080\AppData\Roaming\Python\Python312\site-packages\sqlalchemy\engine\base.py", line 1415, in execute
    return meth(
           ^^^^^
  File "C:\Users\A0885080\AppData\Roaming\Python\Python312\site-packages\sqlalchemy\sql\elements.py", line 523, in _execute_on_connection
    return connection._execute_clauseelement(
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "C:\Users\A0885080\AppData\Roaming\Python\Python312\site-packages\sqlalchemy\engine\base.py", line 1637, in _execute_clauseelement
    ret = self._execute_context(
          ^^^^^^^^^^^^^^^^^^^^^^
  File "C:\Users\A0885080\AppData\Roaming\Python\Python312\site-packages\sqlalchemy\engine\base.py", line 1842, in _execute_context
    return self._exec_single_context(
           ^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "C:\Users\A0885080\AppData\Roaming\Python\Python312\site-packages\sqlalchemy\engine\base.py", line 1982, in _exec_single_context
    self._handle_dbapi_exception(
  File "C:\Users\A0885080\AppData\Roaming\Python\Python312\site-packages\sqlalchemy\engine\base.py", line 2351, in _handle_dbapi_exception
    raise sqlalchemy_exception.with_traceback(exc_info[2]) from e
  File "C:\Users\A0885080\AppData\Roaming\Python\Python312\site-packages\sqlalchemy\engine\base.py", line 1963, in _exec_single_context
    self.dialect.do_execute(
  File "C:\Users\A0885080\AppData\Roaming\Python\Python312\site-packages\sqlalchemy\engine\default.py", line 943, in do_execute
    cursor.execute(statement, parameters)
  File "C:\Users\A0885080\AppData\Roaming\Python\Python312\site-packages\pymysql\cursors.py", line 153, in execute
    result = self._query(query)
             ^^^^^^^^^^^^^^^^^^
  File "C:\Users\A0885080\AppData\Roaming\Python\Python312\site-packages\pymysql\cursors.py", line 322, in _query
    conn.query(q)
  File "C:\Users\A0885080\AppData\Roaming\Python\Python312\site-packages\pymysql\connections.py", line 563, in query
    self._affected_rows = self._read_query_result(unbuffered=unbuffered)
                          ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "C:\Users\A0885080\AppData\Roaming\Python\Python312\site-packages\pymysql\connections.py", line 825, in _read_query_result
    result.read()
  File "C:\Users\A0885080\AppData\Roaming\Python\Python312\site-packages\pymysql\connections.py", line 1199, in read
    first_packet = self.connection._read_packet()
                   ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "C:\Users\A0885080\AppData\Roaming\Python\Python312\site-packages\pymysql\connections.py", line 775, in _read_packet
    packet.raise_for_error()
  File "C:\Users\A0885080\AppData\Roaming\Python\Python312\site-packages\pymysql\protocol.py", line 219, in raise_for_error
    err.raise_mysql_exception(self._data)
  File "C:\Users\A0885080\AppData\Roaming\Python\Python312\site-packages\pymysql\err.py", line 150, in raise_mysql_exception
    raise errorclass(errno, errval)
sqlalchemy.exc.IntegrityError: (pymysql.err.IntegrityError) (1062, "Duplicate entry '53' for key 'source_system.PRIMARY'")
[SQL: INSERT INTO source_system (df_source_system_id, source_system, created_at, modified_at) VALUES (%(df_source_system_id)s, %(source_system)s, %(created_at)s, %(modified_at)s)]
[parameters: {'df_source_system_id': '53ce9c5a-e34a-55a9-b988-dfdd8054aa4f', 'source_system': 'as400_affprd', 'created_at': datetime.datetime(2025, 12, 5, 12, 32, 18, 799321), 'modified_at': datetime.datetime(2025, 12, 5, 12, 32, 18, 799321)}]
(Background on this error at: https://sqlalche.me/e/20/gkpj)
