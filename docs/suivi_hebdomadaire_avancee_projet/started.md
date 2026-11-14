# Démarrage des services

> CONTRAINTES ENVIRONNEMENT DE DEVELOPPEMENT:
> * environnement actuel sous environnement windows (scripts `powershell`)
> * passage impératif sous environnement **Linux Debian** sera réalisé prochainement

## Docker

### Démarrage & vérification services

* **Docker**: à la racine du projet [smart_city](../../smart_city_lyon/), où se trouve le fichier [`docker-compose.yml`]:

```sh
docker compose config
docker compose up -d
docker compose ps
```

* Lancement invite `SQL` pr la vérification de **PostGIS**:

```sh
# ouverture invite psql
docker exec -it smartcity_postgres psql -U smartcity -d smartcity
# \q pr fermer invite après lancement des requêtes ci-dessous
```

```sql
SELECT PostGIS_Full_Version();
```
* puis,
```sql
SELECT schema_name
FROM information_schema.schemata
WHERE schema_name IN ('raw', 'staging', 'analytics');
```

* **Redpanda**
```sh
docker exec -it smartcity_redpanda rpk cluster info
```
* creation &er topics
```sh
docker exec -it smartcity_redpanda rpk topic create traffic.raw
docker exec -it smartcity_redpanda rpk topic list
```

---
### Environnement Python

* si les conteneurs sont démarrés, on passe à la création de l'environnement ``Python``

```sh
py -3.12 -m venv .venv
.\.venv\Scripts\Activate.ps1

python -m pip install --upgrade pip
pip install httpx pydantic-settings python-dotenv confluent-kafka "psycopg[binary]" sqlalchemy geoalchemy2 pandas geopandas pytest ruff
```

---
### Premiers tests

* **Critères de validation de cette 1ère étape (avant de coder les collecteurs)** :

|Test                    |Résultat attendu|
|:-----------------------|:---------------|
|Docker Compose          |Configuration valide|
|Redpanda                |Broker accessible|
|Topic traffic.raw       |Topic créé|
|PostgreSQL              |Connexion réussie|
|PostGIS                 |Extension activée|
|Schémas                 |raw, staging, analytics présents|
|Python                  |Environnement activé et dépendances installées|

