def propagate_pk_info_to_non_pk_rows(**context):
    """
    Propaga TODA la información PK hacia las filas no-PK (is_pk = 0)
    dentro de lookuptable_fk_pk.
    """
    bq_hook = BigQueryHook(
        project_id=project_id,
        location=region,
    )
    client = bq_hook.get_client()

    update_sql = f"""
    UPDATE `{bq_lookup_table}` AS T
    SET
      T.database_name_pk   = P.database_name_pk,
      T.schema_name_pk     = P.schema_name_pk,
      T.table_name_pk      = P.table_name_pk,
      T.column_name_pk     = P.column_name_pk,
      T.is_pk_pk           = P.is_pk_pk,
      T.total_rows_pk      = P.total_rows_pk,
      T.distinct_values_pk = P.distinct_values_pk,
      T.pk_num_fk_columns  = P.pk_num_fk_columns,
      T.pk_size_class      = P.pk_size_class,
      T.pk_name_score      = P.pk_name_score,
      T.pk_avg_fk_ratio    = P.pk_avg_fk_ratio,
      T.pk_lookup_score    = P.pk_lookup_score
    FROM `{bq_lookup_table}` AS P
    WHERE
      -- misma tabla
      T.database_name = P.database_name
      AND T.schema_name = P.schema_name
      AND T.table_name = P.table_name

      -- P es la fila PK
      AND P.is_pk_pk = 1

      -- T es la fila no-PK
      AND T.is_pk = 0

      -- evita reescritura innecesaria
      AND (
        T.database_name_pk IS NULL
        OR T.schema_name_pk IS NULL
        OR T.table_name_pk IS NULL
        OR T.column_name_pk IS NULL
        OR T.is_pk_pk IS NULL
        OR T.total_rows_pk IS NULL
        OR T.distinct_values_pk IS NULL
        OR T.pk_num_fk_columns IS NULL
        OR T.pk_size_class IS NULL
        OR T.pk_name_score IS NULL
        OR T.pk_avg_fk_ratio IS NULL
        OR T.pk_lookup_score IS NULL
      )
    """

    logging.info("Propagating FULL PK info to non-PK rows in lookuptable_fk_pk...")
    client.query(update_sql, location=region).result()
    logging.info("Full PK info propagation completed.")