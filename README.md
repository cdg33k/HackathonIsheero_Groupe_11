# HackathonIsheero Equipe 11
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
- `dashboard` : captures d'écran du dashboard et les fichiers .pbix. Dashboard.pbix le dashboard principale et Dashboard-copie.pbix, le dashboard copié utilisant les données de l'industrialisation. Le lien ici de la vidéo démo du dashboard ici: https://drive.google.com/drive/folders/1eNCG36iUHV006lmpyMIfxz5XypkuwDsi?usp=sharing
- `model` : sauvegarde du modèle produit .
- `pipeline` contenant le pipeline d'industrialisation de données

## Chargement des données dans les notebooks
Les données des notebooks sont chargées directement depuis le repository GitHub afin de garantir la reproductibilité, indépendamment de l'environnement.

## Récupération des données
 
Les données proviennent de deux tables BigQuery du projet GDELT :
- `gdelt-bq.gdeltv2.events` : les événements (acteurs, type, ton, géolocalisation…)
- `gdelt-bq.gdeltv2.gkg` : les thèmes détectés dans les articles et les noms de médias sources
Ces deux tables sont jointes via `SOURCEURL` (events) et `DocumentIdentifier` (gkg). Les données sont chargées directement depuis le repository GitHub pour garantir la reproductibilité.
 
## Nettoyage des données
 
Trois problèmes ont été identifiés et traités.
 
**Bruit géographique** : Le mot "Benin" désigne à la fois le pays et Benin City au Nigeria. Une grande partie des événements récupérés ne concernait pas le Bénin. Un filtrage par mots-clés ("benin city", "edo", "lagos", "nigeria"…) a permis de supprimer ces lignes. Environ 40% des données ont été supprimées. La stratégie adoptée a été de filtrer ces données en se basant sur des mots-clés afin d'obtenir un dataset le plus propre possible.
 
**Doublons miroirs** : GDELT crée deux lignes pour chaque événement impliquant deux pays, en inversant les rôles des acteurs. Ces paires ont des identifiants différents mais représentent le même événement. Elles ont été dédupliquées via une partition sur la date, l'URL source, le code événement et le score de Goldstein.
 
**Mentions accessoires** : Certains articles citaient le Bénin de façon secondaire (ex. "pays voisin du Togo") sans en faire leur sujet principal. Le filtre `IsRootEvent = 1` a été appliqué pour ne conserver que les événements centraux de chaque article.

## Analyse exploratoire & Modèle

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
- **Split temporel** : 80% des données (passé) pour l'entraînement, 20% (futur) pour le test,sans data leakage.
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
 
Le dashboard Power BI est organisé comme suit :
- **Vue d'ensemble** : KPIs globaux (volume d'articles, ton moyen, nombre de sources), évolution temporelle et répartition par origine des médias et par domaine thématique
- **Analyse thématique** : évolution du ton médiatique, stabilité par type d'événement (échelle de Goldstein), ton moyen par domaine
- **Vue opérationnelle** : accès aux 10 événements les plus récents, les plus couverts, les plus déstabilisateurs ou les plus positifs sur une période donnée, avec lien vers l'article source

Vous trouverai une vidéo demo dans le dossier du lien google drive contenant la vidéo de présentation. Le dashboard est totalement fonctionnel et les filtres temporels sont opérationnels!

## Pipeline de mise à jour automatique
 
> ⚠️ Cette partie n'est pas reproductible en l'état, elle nécessite un compte Google Cloud avec facturation activée, ainsi que les droits d'accès à notre projet BigQuery. Elle est présentée ici à titre documentaire pour montrer le travail d'industrialisation réalisé.
 
En marge du hackathon, nous avons été curieux d'avoir des données en live (ce quiétait notre idée depuis le départ) et nous avons donc industrialisé le pipeline sur Google Cloud Platform pour permettre une mise à jour automatique des données, au lieu de travailler uniquement sur un export figé de 2025.
 
**Ce qui a été mis en place :**
 
- Une **Cloud Function Python** (`benin-pipeline-v1`) déployée via Google Cloud Shell. Cette fonction reproduit fidèlement toutes les étapes de transformation appliquées dans notre notebook d'analyse 1 : extraction des fichiers GDELT depuis `gdeltproject.org`, filtrage sur le Bénin, jointure `events` + `gkg`, mapping des thèmes via `Domaine_MAPPING`, assignation des régions médias, et création de la variable `crisis`.
- Pour éviter toute duplication des données dans la table BigQuery benin_final, la Cloud Function supprime automatiquement les lignes existantes pour la date traitée avant d'insérer les nouvelles. Ce mécanisme garantit qu'une exécution multiple de la fonction sur une même date ne génère aucun doublon.
- Nous avons configuré un **Cloud Scheduler** pour déclencher automatiquement cette fonction chaque nuit à 2h00 UTC, assurant ainsi une mise à jour quotidienne sans intervention manuelle.
La table `benin_final` dans BigQuery contient les données 2025 complètes, ainsi que les données du 1er au 5 janvier 2026 et du 9 mai 2026. Nous n'avons pas réalisé de backfill complet de 2026 compte tenu des contraintes de temps.
 
Un second dashboard de démonstration se connecte à cette table `benin_final`  mais n'est pas chargé avec toutes les données, il est là uniquement à titre indicatif.
 
Le code du pipeline est disponible dans `pipeline/main.py`.

## Équipe

Nous sommes sûrs que nous sommes la meilleure équipe du hackathon et nous pouvons faire plus si nous en avons l'opportunité !

**Hackathon Isheero — Groupe 11**

| Rôle | Nom |
|---|---|
| Data Analyst | HOUETO Inès |
| Data Science | SARA GANKOU Alimi |
| ML Engineer | OUEDRAOGO Somtinda Abdoul Khalil |
| ML Engineer | AYETOLOU Cynthia |


NB : Mise en forme du readme réalisé avec l'IA
