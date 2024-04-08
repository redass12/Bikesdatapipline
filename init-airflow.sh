#!/bin/bash
# Initialiser la base de données Airflow
airflow db init

# Créer un utilisateur administrateur
airflow users create \
  --username admin \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email admin@example.com \
  --password admin
