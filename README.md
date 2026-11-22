# Smart City Analytics – Lyon

**Real-time urban data collection, processing and geospatial analysis.**

## Project overview

Smart City Analytics – Lyon is a portfolio project focused on collecting, processing and analyzing urban data in order to better understand the dynamics of a smart city.

The project aims to combine real-time data streams and geospatial analytics to explore topics such as urban mobility, air quality and weather conditions in the Lyon metropolitan area.

## Objectives

- Collect urban data from external APIs.
- Process and stream data using Python and Redpanda.
- Store and manage geospatial data with PostgreSQL/PostGIS.
- Perform temporal and spatial analysis.
- Develop KPIs and interactive visualizations.

## Tech stack

| Technology | Purpose |
|---|---|
| Python | Data collection and processing |
| Redpanda | Real-time data streaming |
| PostgreSQL | Data storage |
| PostGIS | Geospatial data management |
| Pandas / GeoPandas | Data analysis |
| Docker | Local development environment |

## Architecture

The planned data pipeline:

`External APIs → Python Collectors → Redpanda → PostgreSQL/PostGIS → Analytics`

## Project status

**Initial development – Local environment setup.**

The architecture and technology choices have been defined. The next steps are to configure the local Docker environment, validate the infrastructure and develop the first data collectors.

## Scope

Initial geographical scope: Lyon metropolitan area, France.

----
## START PROJECT

## Clone project from repo

---

---

## Create a Python virtual environment

> Windows PowerShell:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
```

Install the project dependencies using the method configured in `pyproject.toml`. For example, if the project is installable:

```powershell
python -m pip install -e .
```

Install pytest if it is not already included in the development dependencies:

```powershell
python -m pip install pytest
```

### 3. Configure environment variables

Create a local `.env` file

---

## Avancement du projet au 17/11/2026

1. **Validé**
    Environnement Docker : PostgreSQL/PostGIS et Redpanda.
2. **Validé**
    Producteur Python → Redpanda.
3. **Validé**
    Consommateur Redpanda → PostgreSQL.
4. **Validé**
    Stockage JSONB et gestion des messages sans coordonnées.
5. **Validé**
    - Création et stockage d'une géométrie PostGIS.
6. **WIP**
    Collecte de données réelles de trafic à Lyon.

--- 
> voir détails des étapes réalisées dans les fichiers de documentation du projet, à retrouver dans le [dossier ".docs"](./docs/)
