# HackathonIsheero Groupe 11
# Analyse des crises au Bénin à partir des données GDELT

## Objectif du projet
Ce projet a pour objectif d'analyser les événements liés au Bénin à partir des données GDELT, afin de :
- Comprendre la dynamique des crises
- Identifier les biais liés au bruit géographique (notamment Nigeria / Benin City)
- Explorer les relations entre variables médiatiques et situations de crise
- Préparer une base propre pour la modélisation

## Structure du projet

Nous avons un dossier :
- `data` contenant les fichiers csv initiaux et finaux, notamment :
  - `data_event` : les données brutes extraites du 1er janvier au 31 décembre 2025
  - `gkg` : les données extraites de la table GKG contenant les sources
  - `database_clean` : la base de données finale après nettoyage des données provenant du Nigeria
  - `data_daily_features` : les données agrégées par jour utilisées pour le modèle
- `notebooks` contenant les notebooks utilisés :
  - `Notebook_1Challenge_isheero__G11` : processus de récupération et de nettoyage des données
  - `notebook-eda-model` : analyse exploratoire des données et entraînement du modèle de prédiction de conflits (voir détails ci-dessous)
- `dashboard` : lien vers notre dashboard interactif
- `model` : sauvegarde du modèle produit

## Chargement des données
Les données des notebooks sont chargées directement depuis le repository GitHub afin de garantir la reproductibilité, indépendamment de l'environnement.

## Nettoyage des données

### Problème identifié
Une ambiguïté géographique a été détectée entre :
- **Bénin (pays)** et
- **Benin City (Nigeria)**

La stratégie adoptée a été de filtrer ces données en se basant sur des mots-clés afin d'obtenir un dataset le plus propre possible.

## Analyse exploratoire & Modèle — `notebook-eda-model`

Ce notebook réalise une analyse exploratoire (EDA) puis entraîne un modèle de machine learning pour **prédire l'apparition d'un conflit au Bénin dans les 7 prochains jours**, à partir des données médiatiques GDELT 2025.

### Analyse exploratoire (EDA)
- **Distribution temporelle** : forte variabilité du volume d'événements, avec un pic en décembre correspondant à la tentative de coup d'État.
- **Ton médiatique** (`AvgTone`) : distribution quasi-normale légèrement négative, traduisant un climat médiatique globalement neutre.
- **Types d'événements** (`QuadClass`) : ~64% de coopération verbale, ~25% de conflits (verbaux + matériels).
- **Intensité des événements** (`GoldsteinScale`) : majorité d'événements neutres à légèrement coopératifs (moyenne de 0.72).

### Feature Engineering
- **Variables temporelles** : jour de la semaine, mois, trimestre, indicateur week-end, encodage cyclique (sin/cos).
- **Encodage des acteurs** : flags binaires pour gouvernement, militaire, acteurs internationaux et civils (Actor1 & Actor2).
- **Encodage des thèmes** : détection par mots-clés des thèmes de crise, élections, économie et sécurité.
- **Agrégation journalière** : toutes les features sont agrégées par jour (moyennes, sommes, taux).
- **Features glissantes (rolling)** : moyennes sur 3, 7 et 14 jours pour le Goldstein et le ton médiatique.
- **Lags** : valeurs J-1 pour le Goldstein, le ton et le statut de crise.
- **Pression médiatique** : ratio mentions/sources.

### Définition de la cible
- `crisis_day` : 1 si plus de 40% des événements du jour sont des conflits.
- `conflit_detect` *(target)* : 1 si au moins un `crisis_day` est détecté dans les **7 jours suivants**.

### Modélisation
- **Split temporel** : 80% des données (passé) pour l'entraînement, 20% (futur) pour le test — sans data leakage.
- **Gestion du déséquilibre** : pondération des classes (`class_weight='balanced'`).
- Deux modèles entraînés et comparés :
  - **Random Forest** (`n_estimators=200`, `max_depth=10`)
  - **Régression Logistique** (`solver='lbfgs'`, `max_iter=1000`)
- Évaluation via rapport de classification et **AUC-ROC**.

### Résultats
Le modèle Random Forest détecte correctement la montée de risque en décembre 2025, avec une probabilité de conflit dépassant le seuil de 50% **plusieurs jours avant** les événements réels liés à la tentative de coup d'État.

### Sorties du notebook
| Fichier | Contenu |
|---|---|
| `random_forest_conflit.pkl` | Modèle Random Forest sauvegardé |
| `data_daily_features.csv` | Dataset journalier avec toutes les features |

## Dashboard
Un dashboard interactif permet d'explorer les données et les résultats du modèle.

## Équipe

Nous sommes sûrs que nous sommes la meilleure équipe du hackathon et nous pouvons faire plus si nous en avons l'opportunité !

**Hackathon Isheero — Groupe 11**

| Rôle | Nom |
|---|---|
| Data Analyst | HOUETO Inès |
| Data Science | SARA GANKOU Alimi |
| ML Engineer | OUEDRAOGO Somtinda Abdoul Khalil |
| ML Engineer | AYETOLOU Cynthia |
