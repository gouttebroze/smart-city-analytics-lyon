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
