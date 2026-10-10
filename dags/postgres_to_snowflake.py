from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook


# Configurações padrão das tarefas da DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
}


@dag(
    dag_id='postgres_to_snowflake',
    default_args=default_args,
    description='Load data incrementally from Postgres to Snowflake',
    schedule=timedelta(days=1),
    catchup=False
)
def postgres_to_snowflake_etl():

    # Tabelas que serão carregadas incrementalmente
    table_names = [
        'veiculos', 'estados', 'cidades',
        'concessionarias', 'vendedores',
        'clientes', 'vendas'
    ]

    for table_name in table_names:

        # Consulta o maior ID já carregado no Snowflake
        @task(task_id=f'get_max_id_{table_name}')
        def get_max_primary_key(table_name: str):
            with SnowflakeHook(snowflake_conn_id='snowflake').get_conn() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        f"SELECT MAX(ID_{table_name}) FROM {table_name}"
                    )
                    max_id = cursor.fetchone()[0]

                    return max_id if max_id is not None else 0

        # Extrai apenas novos registros do PostgreSQL e os insere no Snowflake
        @task(task_id=f'load_data_{table_name}')
        def load_incremental_data(table_name: str, max_id: int):
            with PostgresHook(postgres_conn_id='postgres').get_conn() as pg_conn:
                with pg_conn.cursor() as pg_cursor:

                    primary_key = f'ID_{table_name}'

                    # Identifica as colunas da tabela de origem
                    pg_cursor.execute(
                        f"""
                        SELECT column_name
                        FROM information_schema.columns
                        WHERE table_name = '{table_name}'
                        """
                    )
                    columns = [row[0] for row in pg_cursor.fetchall()]
                    columns_list_str = ', '.join(columns)
                    placeholders = ', '.join(['%s'] * len(columns))

                    # Extrai somente registros com ID maior que o último carregado
                    pg_cursor.execute(
                        f"""
                        SELECT {columns_list_str}
                        FROM {table_name}
                        WHERE {primary_key} > {max_id}
                        """
                    )
                    rows = pg_cursor.fetchall()

                    # Insere os registros extraídos no Snowflake
                    with SnowflakeHook(snowflake_conn_id='snowflake').get_conn() as sf_conn:
                        with sf_conn.cursor() as sf_cursor:
                            insert_query = (
                                f"INSERT INTO {table_name} "
                                f"({columns_list_str}) VALUES ({placeholders})"
                            )

                            for row in rows:
                                sf_cursor.execute(insert_query, row)

        # Define a dependência entre a consulta do ID e a carga incremental
        max_id = get_max_primary_key(table_name)
        load_incremental_data(table_name, max_id)


# Registra a DAG para que o Airflow possa identificá-la
postgres_to_snowflake_etl_dag = postgres_to_snowflake_etl()