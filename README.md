![Python](https://img.shields.io/badge/python-v3.8+-blue.svg)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](https://opensource.org/licenses/MIT)

# Football Tournaments Data Archive

<a href="http://euro-stat.com/" target="_blank">LINK TO THE PROJECT</a>
<img src="images/app.png" alt="App Preview" width="500">

<a href="https://www.youtube.com/watch?v=ld8AY3CgSn4" target="_blank">
Widget logic (video)</a>


## Table of Contents

- [Project Description](#project-description)
- [Technologies](#technologies)
- [Data Pipeline Architecture and Workflow](#data-pipeline-architecture-and-workflow)
  - [(0) Data sources](#0-data-sources)
  - [(1) Ingest historical and moving-forward data to Yandex object storage](#ingest-historical-and-moving-forward-data-to-yandex-object-storage)
  - [(2) Pandas loads data from Yandex Object Storage](#2-pandas-loads-data-from-cloud-storage)
  - [(3) Data Warehouse Transformation with dbt and (6) prefect to schedule incremental transformation](#3-data-warehouse-transformation-with-dbt-and-6-prefect-to-schedule-incremental-transformation)
  - [(4) Spark data analytics](#4-spark-data-analytics)
  - [(5) Custom Data Visualization](#4-custom data visualization)
  
- [Reproducability](#reproducability)
  - [Step 1: Build GCP Resources from Local Computer](#step-1-build-gcp-resources-from-local-computer)
  - [Step 2: Setup Workaround on VM](#step-2-setup-workaround-on-vm)
- How to use the visualization?
- [Further Improvements](#further-improvements)


## Project Description

This project showcases best practices from [Data Engineering Zoomcamp](https://github.com/DataTalksClub/data-engineering-zoomcamp) course.
The data contains all the basic information about the Football World and European Championships, starting from the very first official World Cup held in Uruguay in 1930.
You can learn about the time and locations of the championships, the participating countries, and the match results here.
Additionally, the project contains details about each individual game, including the time the goals were scored.
Real-time data for the current championship is also displayed.
The visualization was developed separately using the Flask framework and is available at <a href="http://euro-stat.com/" target="_blank">http://euro-stat.com</a>.

# Technologies

- Yandex object storage as the datalake to store our raw dataset.
- Mysql as the data warehouse.
- dbt core as the transformation tool to implement data modeling.
- Self-hosted Prefect core to manage and monitor our workflow.
- Terraform to easily manage the infrastructure setup and changes.
- Hertzner VPS as the virtual host to host our data pipeline.
- Flask + HTML + JS + Jinja for data visualisation

with some improvements to support easy reproducability, such as:
  - Containerized environment with docker


# Data Pipeline Architecture and Workflow
```mermaid
flowchart LR

style A fill:#6ebcff,stroke:#333,stroke-width:2px,stroke-dasharray: 5, 5;
style B fill:#ffa07a,stroke:#333,stroke-width:2px,stroke-dasharray: 5, 5;
style C fill:#ffc93c,stroke:#333,stroke-width:2px,stroke-dasharray: 5, 5;
style S fill:#bfc93c,stroke:#333,stroke-width:2px,stroke-dasharray: 5, 5;
style Y fill:#bfc93c,stroke:#333,stroke-width:2px,stroke-dasharray: 5, 5;

subgraph "Prefect"
Z(Docker)
X(Terraform)
subgraph "Data Sources"
A(FIFA)
B(UEFA)
C(SPORTS.RU)
S(clubelo.com)
Y(eloratings.net)

end



    subgraph "Data Lake"
        E(Yandex Object Storage)
        
    end

    subgraph "Dataware house"
        F(MYSQL-staging)
        subgraph "Data Transformation"
        G(DBT)
        end
        L(MYSQL-prod)
    end
end


subgraph "Real-time Streaming"
H(Kafka)
I(MongoDB)
end

subgraph "Data Analytics"
J(Spark)
end

A -- 1. ingest raw data with python--> E 
B -- 1. ingest raw data with python--> E
C -- 1. ingest raw data with python--> E
S -- 1. ingest raw data with python--> E
Y -- 1. ingest raw data with python--> E
E -- 2. Daily --> F
F -- 3. Daily --> G
G -- 3. Transformed Data --> L
C -- Real-time --> H
H -- Last Game Statistics  --> I
E -- 4. Raw Data --> J
```

### (1) Ingest historical and moving-forward data to Yandex object storage

The system ingests data from 5 different sources. Historical data covers the data that are created before the ingestion date. Moving-forward data is data that are updated daily.

Prefect is used to run both types of data and to schedule daily ingestion for moving-forward data.

Yandex Object Storage is our datalake. The data is stored in the datalake as Parquet files, 

For the game statistics it is partitioned by Tournament, Year, Stage Name and Date.

```{source}/football/{tournament_id}/{year}/{stage_name}/{date}```

![yandex object storage](/images/ya_object_storage.png)


### (2) Pandas loads data from Yandex Object Storage

Raw data is loaded into the corresponding staging tables in the databases fifa_staging, uefa_staging, sports_ru_staging, elorating_staging, clubelo_staging. 
The tables may contain duplicates.

### (3)  Data Warehouse Transformation with dbt

The second layer of the data pipeline consists of staging tables.
We have staging tables for game results, schedules, stadiums, and statistics. For staging tables, we remove duplicates using additional views. Staging tables are cleared for a period of more than N days.
We use these staging tables to update the production tables in the euro_stat database. We update game results, add new players, teams e.t.c.



#### (3.1) Datawarehouse (MYSQL  mysql:8.0.27)

Main tables related to each other by foreign keys, with a star schema and a fact table matches:
DDL tables are stored in  `03_datawarehouse_mysql/ddl`

1. Competitions. The considered tournament.
2. Teams. World Cup teams, team icons URL and country code (needed for rendering a world map).
3. Matches (fact table). Played or scheduled matches, goals scored/missed, nominal home team/nominal away team.
4. Stages. All rounds. Group stage/quarterfinals/semifinals/final or rounds for club championships.
5. Places. The location where the match was played, stadium, stadium capacity, stadium name, and location on the world map.
6. Players. Players of teams, their position on the field, age, height.
7. Goals. Goals scored in the match, for which team, and at which minute.




```mermaid
erDiagram
    teams {
        id bigint PK
        short_name text
        short_name2 text
        name text
    }

    stages {
        id int PK
        title text
    }

    players {
        id int PK
        number int
        image text
        first_name text
        last_name text
        position text
        team_id int FK
        birthdate date
    }

    places {
        id bigint PK
        stadium text
        city text
        short_name text
        capacity int
        competition_id bigint FK
        lat double
        lng double
    }

    match_status {
        id int PK
        status text
    }

    matches {
        id int PK
        competition_id bigint FK
        place_id bigint FK
        stage_id int FK
        status_id int FK
        date text
        local_date text
        home_team_id bigint FK
        away_team_id bigint FK
        goals_home_team int
        goals_away_team int
        total_home_goals int
        total_away_goals int
        penalty_shootout_home_goals int
        penalty_shootout_away_goals int
        game_order int
    }

    goals {
        id bigint PK
        match_id bigint FK
        player_id bigint FK
        team_id bigint FK
        minute int
        second int
        text text
    }

    competitions {
        id bigint PK
        number_of_games bigint
        number_of_match_days bigint
        number_of_teams bigint
        last_updated text
        caption text
        year int
        league text
        url text
    }

    teams }|..|| players : have
    matches ||--|| places: played_at
    matches ||--|| competitions : belongs_to
    matches ||--|| teams : home
    matches ||--|| teams : away
    matches ||--|| places : played_at
    matches ||--|| stages : belongs_to
    matches ||--|| match_status : has_status
    goals }|..|| matches : scored_in
    goals }|..|| players : scored_by
    goals }|..|| teams : scored_for

```

### (4) Spark data analytics.

We need Spark for further work to run machine learning models and make predictions. Currently, we are examining simple metrics for the number of goals by championship and season.


### (5) Custom Data Visualization.
The program interface consists of an elliptical diagram with sets of slices, with each slice representing a different data group such as teams, match schedules, stadiums and cities, groups, and stages of the tournament.

Clicking on each slice of the diagram highlights the associated data structure with the event. Specifically, clicking on:

* Team highlights the days on which the matches will be played, the stadiums, and the groups. The center displays the schedule of all matches.
* Match day highlights all teams playing on that day, the stadiums, and the groups. The center displays the schedule of all matches on that day.
* Stadium
* Stage

In addition to the local slices, there are events when clicking on the external arches:

1. Clicking on "National teams" shows a world map with the participating countries highlighted.
2. Clicking on "Schedule" displays a calendar of all game days.
3. Clicking on "Cities and stadiums" shows marked cities on the map of the host country of the tournament.
4. Clicking on "Groups and stages" displays the full tournament bracket.





## Further Improvements
There are many things can be improved from this project:

- Implement CI/CD
- Do a comprehensive testing
- Add predictions for games that have not yet been played.
- Convert the widget to the Flutter engine to make the visualization engine cross-platform.