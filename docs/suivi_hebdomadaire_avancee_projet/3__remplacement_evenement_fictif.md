# Remplacement événement fictif par données réelles de circulation

> **Objectif**: remplacer l'événement fictif ``traffic.test`` par des données réelles de circulation de la Métropole de Lyon.

> 2 **Etapes**: identifier et comprendre la source de données, puis développer un collecteur Python qui publiera les événements dans Redpanda 
    
* On conserve le consommateur ``PostgreSQL/PostGIS`` qui a été validé

## Étape 1 — Identifier la source de données de trafic

### Construction du premier pipeline

---
                -- 1 --     
        Open Data – Métropole de Lyon
        Données réelles de circulation
---

---
                -- 2 --
        Collecteur Python
        Récupération, validation et normalisation
---

---
                -- 3 --
        Redpanda — traffic.raw
        Transport des événements
---

---
                -- 4 --
        Consumer Python
        Déjà développé et testé
---

---
                -- 5 --
        PostgreSQL / PostGIS
        Stockage des événements géolocalisés
---

### Source retenue : l'état du trafic de la Métropole de Lyon
