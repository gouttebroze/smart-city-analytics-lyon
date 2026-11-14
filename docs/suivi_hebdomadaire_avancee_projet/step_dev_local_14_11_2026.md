> Step développement local 

> Réalisation au 14/11/2026

# Suivi hebdomadaire – Smart City Analytics · Lyon

1. Objectifs et périmètre

> Construction projet Data Analytics orienté Smart City, prenant Lyon comme territoire d'expérimentation.

* Les **objectifs** sont de :

    - Collecter des données urbaines en temps réel ou quasi réel.
    - Construire une chaîne d'ingestion et de traitement avec Python et Redpanda.
    - Stocker les données dans PostgreSQL/PostGIS.
    - Réaliser des analyses temporelles et géospatiales.
    - Produire des indicateurs exploitables dans un tableau de bord.
    - Préparer ultérieurement une application Android.

## Etat des issues: 

* Ttes faites sauf la connection Python à PostgreSQL/PostGIS

* Validation du premier échange de données ds  l'architecture Smart City.

|Composant|	Résultat|
|:--------|:--------|
|Docker Compose |	Validé|
|PostgreSQL |	Healthy|
|PostGIS 3.5.2  |	Validé|
|Package Python |	Validé|
|Topic traffic.raw  |	Validé|
|Producer Python    |	Validé|
|Consumer Python    |	Validé|
|Insertion PostgreSQL depuis Python |	À tester|

> **Test sur Streaming traffic connection**: L'**offset 1** indique que le **message** a été enregistré à la **position 1 de sa partition**, ça ne représente pas une erreur.

### Connection Python à PostgreSQL/PostGIS