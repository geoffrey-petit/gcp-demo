from astronomer_starship.providers.starship.operators.starship import StarshipAirflowMigrationDAG

globals()['starship_airflow_migration_dag'] = StarshipAirflowMigrationDAG(http_conn_id="starship_default")