# HackathonIsheero Groupe 11

# Analyse des crises au Bénin à partir des données GDELT

## Objectif du projet

Ce projet a pour objectif d’analyser les événements liés au Bénin à partir des données GDELT, afin de :

- Comprendre la dynamique des crises
- Identifier les biais liés au bruit géographique (notamment Nigeria / Benin City)
- Explorer les relations entre variables médiatiques et situations de crise
- Préparer une base propre pour la modélisation


##  Structure du projet
Nous avons un dossier : 
- data contenant les fichiers csv initiaux et finaux, notamment :
    - data_event qui sont les données brutes extraites du 1er Janvier au 31 décembre 2025
    - gkg: qui sont les données extraites de la table gkg contenant les sources;
    - database_clean: la base de donnée finale avec nettoyage des données provenant du Nigeria.
    - et data_daily_features :  Les données agrégées par jour utilisées pour le modèle;

- notebooks contenant les notebooks utilisés : 
    - Notebook_1Challenge_isheero__G11 qui contient le processus de récupération et de nettoyage des données
    -  Notebookk-eda-model qui contient ------
- dashboard qui contient le lien vers notre dashboard

## Chargement des données

Les données du notebook sont chargées directement depuis le repository GitHub afin de garantir la reproductibilité, indépendamment de l’environnement

##  Nettoyage des données

- Problème identifié

Une ambiguïté géographique a été détectée entre :

- **Benin (pays)** et
- **Benin City (Nigeria)**

La stratégie adoptée a été de filtrer ces données en se bansant sur des mots clés afin d'avoir un dataset le plus propre possible

## Modèle (à venir)

Un modèle de machine learning sera ajouté pour ---.


##  Dashboard (à venir)

Un dashboard interactif permettra d’explorer les données. décembre 

## Équipe
Nous somme sûrs que nous sommes la meilleure équipe du hackathon et nous pouvons faire plus si nous en avons l'opportunité !

Hackathon Isheero — Groupe 11


Data Analyst --	HOUETO Inès

Data Science --	SARA GANKOU Alimi

ML Engineer --	OUEDRAOGO Somtinda Abdoul Khalil

ML Engineer	-- AYETOLOU Cynthia


