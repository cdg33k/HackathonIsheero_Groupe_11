import functions_framework
import pandas as pd
import requests
import zipfile
import io
from google.cloud import bigquery
from datetime import datetime, timedelta

# ============================================================
# DICTIONNAIRE MAPPING DOMAINES
# ============================================================

Domaine_MAPPING = {
    'Politique & Gouvernance': [
        'USPEC_POLITICS_GENERAL', 'GENERAL_GOVERNMENT', 'LEADER',
        'LEGISLATION', 'WB_831_GOVERNANCE', 'WB_696_PUBLIC_SECTOR',
        'WB_832_ANTI_CORRUPTION', 'WB_678_DIGITAL_GOVERNMENT',
        'ELECTION', 'TAX_FNCACT_PRESIDENT', 'TAX_FNCACT_MINISTER',
        'USPEC_POLICY', 'CONSTITUTIONAL', 'DEMOCRACY',
        'TAX_FNCACT_GOVERNOR', 'TAX_FNCACT_CHAIRMAN',
        'TAX_FNCACT_AUTHORITIES', 'TAX_FNCACT_CHIEFS_OF_STAFF',
        'TAX_FNCACT_COMPTROLLER'
    ],
    'Sécurité & Conflit': [
        'ARMEDCONFLICT', 'KILL', 'ARREST', 'SECURITY_SERVICES',
        'CRISISLEX_C07_SAFETY', 'CRISISLEX_T03_DEAD', 'CRISISLEX_T02_INJURED',
        'WB_2432_FRAGILITY_CONFLICT', 'WB_2433_CONFLICT_AND_VIOLENCE',
        'TAX_FNCACT_POLICE', 'TAX_MILITARY_TITLE', 'MANMADE_DISASTER_IMPLIED',
        'SOC_GENERALCRIME', 'WB_1014_CRIMINAL_JUSTICE', 'TRIAL',
        'MILITARY', 'BORDER', 'TAX_FNCACT_SOLDIERS',
        'AVIATION_INCIDENT', 'MANMADE_DISASTER'
    ],
    'Économie': [
        'EPU_ECONOMY', 'EPU_POLICY', 'TAX_ECON_PRICE',
        'WB_1921_PRIVATE_SECTOR', 'WB_2670_JOBS', 'EPU_ECONOMY_HISTORIC',
        'WB_698_TRADE', 'WB_2936_GOLD', 'WB_507_ENERGY',
        'WB_895_MINING', 'WB_1699_METAL', 'TAX_FNCACT_TRADERS',
        'TAX_FNCACT_CASHIER'
    ],
    'Santé': [
        'GENERAL_HEALTH', 'MEDICAL', 'TAX_DISEASE',
        'WB_621_HEALTH', 'WB_635_PUBLIC_HEALTH',
        'CRISISLEX_C03_WELLBEING_HEALTH'
    ],
    'Éducation & Social': [
        'EDUCATION', 'WB_470_EDUCATION', 'TAX_FNCACT_STUDENTS',
        'TAX_FNCACT_CITIZENS', 'TAX_FNCACT_WOMEN', 'TAX_FNCACT_CHILDREN',
        'TAX_FNCACT_ARTISTS', 'FOOD_SECURITY', 'SOC_POINTSOFINTEREST',
        'CRISISLEX_CRISISLEXREC'
    ],
    'Médias & Communication': [
        'MEDIA_MSM', 'WB_694_BROADCAST', 'WB_133_INFORMATION',
        'TAX_WORLDLANGUAGES'
    ],
    'Environnement & Catastrophes': [
        'NATURAL_DISASTER', 'CRISISLEX_O01_WEATHER',
        'CRISISLEX_T01_CAUTION', 'NATURAL_DISASTER_FLOODING',
        'UNGP_FORESTS_RIVERS_OCEANS'
    ],
    'Migration': ['EPU_CATS_MIGRATION'],
}

# ============================================================
# DICTIONNAIRE MÉDIAS
# ============================================================

MEDIAS_CONNUS = {
    '24haubenin.info': 'Afrique de l\'Ouest',
    'benininfo.com': 'Afrique de l\'Ouest',
    'bentelevision.com': 'Afrique de l\'Ouest',
    'gouv.bj': 'Afrique de l\'Ouest',
    'lanouvelletribune.info': 'Afrique de l\'Ouest',
    'levenementprecis.com': 'Afrique de l\'Ouest',
    'visages-du-benin.com': 'Afrique de l\'Ouest',
    'la-flamme.org': 'Afrique de l\'Ouest',
    'tamtaminfo.com': 'Afrique de l\'Ouest',
    'telegramme228.com': 'Afrique de l\'Ouest',
    'punchng.com': 'Afrique de l\'Ouest',
    'guardian.ng': 'Afrique de l\'Ouest',
    'thecable.ng': 'Afrique de l\'Ouest',
    'premiumtimesng.com': 'Afrique de l\'Ouest',
    'dailytrust.com': 'Afrique de l\'Ouest',
    'dailypost.ng': 'Afrique de l\'Ouest',
    'channelstv.com': 'Afrique de l\'Ouest',
    'bellanaija.com': 'Afrique de l\'Ouest',
    'pulse.ng': 'Afrique de l\'Ouest',
    'blueprint.ng': 'Afrique de l\'Ouest',
    'thisdaylive.com': 'Afrique de l\'Ouest',
    'leadership.ng': 'Afrique de l\'Ouest',
    'saharareporters.com': 'Afrique de l\'Ouest',
    'ripplesnigeria.com': 'Afrique de l\'Ouest',
    'thenationonlineng.net': 'Afrique de l\'Ouest',
    'arise.tv': 'Afrique de l\'Ouest',
    'icirnigeria.org': 'Afrique de l\'Ouest',
    'thesun.ng': 'Afrique de l\'Ouest',
    'tell.ng': 'Afrique de l\'Ouest',
    'thenet.ng': 'Afrique de l\'Ouest',
    'thewhistler.ng': 'Afrique de l\'Ouest',
    'naija247news.com': 'Afrique de l\'Ouest',
    'naijanews.com': 'Afrique de l\'Ouest',
    'nairametrics.com': 'Afrique de l\'Ouest',
    'informationng.com': 'Afrique de l\'Ouest',
    'nigerianobservernews.com': 'Afrique de l\'Ouest',
    'nigeriasun.com': 'Afrique de l\'Ouest',
    'nigerianeye.com': 'Afrique de l\'Ouest',
    'nigeriaworld.com': 'Afrique de l\'Ouest',
    'newnigerianpolitics.com': 'Afrique de l\'Ouest',
    'onlinenigeria.com': 'Afrique de l\'Ouest',
    'pmnewsnigeria.com': 'Afrique de l\'Ouest',
    'politicsnigeria.com': 'Afrique de l\'Ouest',
    'truthnigeria.com': 'Afrique de l\'Ouest',
    'tribuneonlineng.com': 'Afrique de l\'Ouest',
    'ynaija.com': 'Afrique de l\'Ouest',
    'newsverge.com': 'Afrique de l\'Ouest',
    'newtelegraphng.com': 'Afrique de l\'Ouest',
    'peoplesdailyng.com': 'Afrique de l\'Ouest',
    'igberetvnews.com': 'Afrique de l\'Ouest',
    'hallmarknews.com': 'Afrique de l\'Ouest',
    'silverbirdtv.com': 'Afrique de l\'Ouest',
    'asabametro.com': 'Afrique de l\'Ouest',
    'obalandmagazine.com': 'Afrique de l\'Ouest',
    'stelladimokokorkus.com': 'Afrique de l\'Ouest',
    'nationalaccordnewspaper.com': 'Afrique de l\'Ouest',
    'thetidenewsonline.com': 'Afrique de l\'Ouest',
    'thenigerianvoice.com': 'Afrique de l\'Ouest',
    'ghanaweb.com': 'Afrique de l\'Ouest',
    'myjoyonline.com': 'Afrique de l\'Ouest',
    'modernghana.com': 'Afrique de l\'Ouest',
    'techcabal.com': 'Afrique de l\'Ouest',
    'tech-ish.com': 'Afrique de l\'Ouest',
    'abidjan.net': 'Afrique de l\'Ouest',
    'fratmat.info': 'Afrique de l\'Ouest',
    'koaci.com': 'Afrique de l\'Ouest',
    'icilome.com': 'Afrique de l\'Ouest',
    'republicoftogo.com': 'Afrique de l\'Ouest',
    'lefaso.net': 'Afrique de l\'Ouest',
    'burkina24.com': 'Afrique de l\'Ouest',
    'actuniger.com': 'Afrique de l\'Ouest',
    'lesahel.org': 'Afrique de l\'Ouest',
    'journaldumali.com': 'Afrique de l\'Ouest',
    'maliactu.net': 'Afrique de l\'Ouest',
    'malijet.com': 'Afrique de l\'Ouest',
    'maliweb.net': 'Afrique de l\'Ouest',
    'bamada.net': 'Afrique de l\'Ouest',
    'lequotidien.sn': 'Afrique de l\'Ouest',
    'lesoleil.sn': 'Afrique de l\'Ouest',
    'senenews.com': 'Afrique de l\'Ouest',
    'seneweb.com': 'Afrique de l\'Ouest',
    'senego.com': 'Afrique de l\'Ouest',
    'dakaractu.com': 'Afrique de l\'Ouest',
    'pressafrik.com': 'Afrique de l\'Ouest',
    'leral.net': 'Afrique de l\'Ouest',
    'africaguinee.com': 'Afrique de l\'Ouest',
    'guineematin.com': 'Afrique de l\'Ouest',
    'slguardian.org': 'Afrique de l\'Ouest',
    'sierraleonetimes.com': 'Afrique de l\'Ouest',
    'frontpageafricaonline.com': 'Afrique de l\'Ouest',
    'thenewdawnliberia.com': 'Afrique de l\'Ouest',
    'thepoint.gm': 'Afrique de l\'Ouest',
    'ami.mr': 'Afrique de l\'Ouest',
    'allafrica.com': 'Panafricain',
    'panapress.com': 'Panafricain',
    'apanews.net': 'Panafricain',
    'africanews.com': 'Panafricain',
    'afrik.com': 'Panafricain',
    'cnbcafrica.com': 'Panafricain',
    'financialafrik.com': 'Panafricain',
    'africa-confidential.com': 'Panafricain',
    'pambazuka.org': 'Panafricain',
    'cameroon-tribune.cm': 'Afrique Centrale',
    'adiac-congo.com': 'Afrique Centrale',
    'mediacongo.net': 'Afrique Centrale',
    'radiookapi.net': 'Afrique Centrale',
    'gabonactu.com': 'Afrique Centrale',
    'gabonews.com': 'Afrique Centrale',
    'standardmedia.co.ke': 'Afrique de l\'Est',
    'capitalfm.co.ke': 'Afrique de l\'Est',
    'citizen.digital': 'Afrique de l\'Est',
    'ena.et': 'Afrique de l\'Est',
    'dailymaverick.co.za': 'Afrique Australe',
    'ewn.co.za': 'Afrique Australe',
    'bulawayo24.com': 'Afrique Australe',
    'mmegi.bw': 'Afrique Australe',
    'aljazeera.com': 'Moyen-Orient & Afrique du Nord',
    'aljazeera.net': 'Moyen-Orient & Afrique du Nord',
    'arabnews.com': 'Moyen-Orient & Afrique du Nord',
    'gulf-times.com': 'Moyen-Orient & Afrique du Nord',
    'presstv.ir': 'Moyen-Orient & Afrique du Nord',
    'zawya.com': 'Moyen-Orient & Afrique du Nord',
    'rfi.fr': 'France & Francophonie',
    'lemonde.fr': 'France & Francophonie',
    'lefigaro.fr': 'France & Francophonie',
    'jeuneafrique.com': 'France & Francophonie',
    'bfmtv.com': 'France & Francophonie',
    'courrierinternational.com': 'France & Francophonie',
    'la-croix.com': 'France & Francophonie',
    'lalibre.be': 'France & Francophonie',
    'letemps.ch': 'France & Francophonie',
    'mondafrique.com': 'France & Francophonie',
    'voltairenet.org': 'France & Francophonie',
    'linfo.re': 'France & Francophonie',
    'dw.com': 'Europe',
    'euronews.com': 'Europe',
    'elpais.com': 'Europe',
    'efe.com': 'Europe',
    'ilsole24ore.com': 'Europe',
    'rt.com': 'Russie & Chine',
    'sputnikglobe.com': 'Russie & Chine',
    'sputniknews.africa': 'Russie & Chine',
    'tass.com': 'Russie & Chine',
    'xinhuanet.com': 'Russie & Chine',
    'chinadaily.com.cn': 'Russie & Chine',
    'globaltimes.cn': 'Russie & Chine',
    'cnn.com': 'Amérique du Nord',
    'foxnews.com': 'Amérique du Nord',
    'npr.org': 'Amérique du Nord',
    'newsweek.com': 'Amérique du Nord',
    'forbes.com': 'Amérique du Nord',
    'voanews.com': 'Amérique du Nord',
    'voaafrique.com': 'Amérique du Nord',
    'bbc.com': 'Amérique du Nord',
    'yahoo.com': 'Amérique du Nord',
    'bostonglobe.com': 'Amérique du Nord',
    'latimes.com': 'Amérique du Nord',
    'bbc.co.uk': 'Royaume-Uni',
    'theguardian.com': 'Royaume-Uni',
    'independent.co.uk': 'Royaume-Uni',
    'sky.com': 'Royaume-Uni',
    'trtworld.com': 'Royaume-Uni',
    'fao.org': 'Organisations internationales',
    'undp.org': 'Organisations internationales',
    'unesco.org': 'Organisations internationales',
    'ecowas.int': 'Organisations internationales',
    'msf.org': 'Organisations internationales',
    'rsf.org': 'Organisations internationales',
    'occrp.org': 'Organisations internationales',
    'globo.com': 'Amérique Latine',
    'telesurtv.net': 'Amérique Latine',
    'infobae.com': 'Amérique Latine',
    'lanacion.com.ar': 'Amérique Latine',
    'straitstimes.com': 'Asie',
    'scmp.com': 'Asie',
    'thehindu.com': 'Asie',
    'dawn.com': 'Asie',
    'ndtv.com': 'Asie',
    'antaranews.com': 'Asie',
}

EXTENSION_REGION = {
    '.co.uk': 'Royaume-Uni', '.uk': 'Royaume-Uni',
    '.ie': 'Royaume-Uni', '.au': 'Royaume-Uni', '.nz': 'Royaume-Uni',
    '.bj': 'Afrique de l\'Ouest', '.ng': 'Afrique de l\'Ouest',
    '.gh': 'Afrique de l\'Ouest', '.sn': 'Afrique de l\'Ouest',
    '.ci': 'Afrique de l\'Ouest', '.ml': 'Afrique de l\'Ouest',
    '.bf': 'Afrique de l\'Ouest', '.ne': 'Afrique de l\'Ouest',
    '.tg': 'Afrique de l\'Ouest', '.gn': 'Afrique de l\'Ouest',
    '.sl': 'Afrique de l\'Ouest', '.lr': 'Afrique de l\'Ouest',
    '.mr': 'Afrique de l\'Ouest', '.gm': 'Afrique de l\'Ouest',
    '.cm': 'Afrique Centrale', '.cg': 'Afrique Centrale',
    '.cd': 'Afrique Centrale', '.ga': 'Afrique Centrale',
    '.td': 'Afrique Centrale', '.ao': 'Afrique Centrale',
    '.ke': 'Afrique de l\'Est', '.tz': 'Afrique de l\'Est',
    '.ug': 'Afrique de l\'Est', '.et': 'Afrique de l\'Est',
    '.rw': 'Afrique de l\'Est',
    '.za': 'Afrique Australe', '.zw': 'Afrique Australe',
    '.bw': 'Afrique Australe', '.mz': 'Afrique Australe',
    '.ma': 'Moyen-Orient & Afrique du Nord',
    '.dz': 'Moyen-Orient & Afrique du Nord',
    '.tn': 'Moyen-Orient & Afrique du Nord',
    '.eg': 'Moyen-Orient & Afrique du Nord',
    '.il': 'Moyen-Orient & Afrique du Nord',
    '.sa': 'Moyen-Orient & Afrique du Nord',
    '.ae': 'Moyen-Orient & Afrique du Nord',
    '.kw': 'Moyen-Orient & Afrique du Nord',
    '.qa': 'Moyen-Orient & Afrique du Nord',
    '.ir': 'Moyen-Orient & Afrique du Nord',
    '.fr': 'France & Francophonie', '.lu': 'France & Francophonie',
    '.de': 'Europe', '.it': 'Europe', '.es': 'Europe',
    '.pt': 'Europe', '.nl': 'Europe', '.pl': 'Europe',
    '.ro': 'Europe', '.bg': 'Europe', '.gr': 'Europe',
    '.at': 'Europe', '.se': 'Europe', '.no': 'Europe',
    '.dk': 'Europe', '.fi': 'Europe', '.cz': 'Europe',
    '.hu': 'Europe', '.hr': 'Europe', '.rs': 'Europe',
    '.ru': 'Russie & Chine', '.cn': 'Russie & Chine',
    '.ca': 'Amérique du Nord',
    '.br': 'Amérique Latine', '.mx': 'Amérique Latine',
    '.ar': 'Amérique Latine', '.ve': 'Amérique Latine',
    '.co': 'Amérique Latine', '.cu': 'Amérique Latine',
    '.uy': 'Amérique Latine', '.py': 'Amérique Latine',
    '.cl': 'Amérique Latine', '.pe': 'Amérique Latine',
    '.in': 'Asie', '.jp': 'Asie', '.kr': 'Asie',
    '.pk': 'Asie', '.id': 'Asie', '.my': 'Asie',
    '.ph': 'Asie', '.vn': 'Asie', '.tw': 'Asie',
    '.bd': 'Asie', '.tr': 'Asie', '.sg': 'Asie',
    '.th': 'Asie', '.az': 'Asie', '.am': 'Asie',
}

# ============================================================
# FONCTIONS DE TRANSFORMATION
# ============================================================

def extraire_domaine(themes_str):
    if pd.isna(themes_str):
        return 'Autre'
    themes_str = themes_str.upper()
    for domaine, mots_cles in Domaine_MAPPING.items():
        for mot in mots_cles:
            if mot in themes_str:
                return domaine
    return 'Politique & Gouvernance'

def associer_region(media):
    if pd.isna(media) or media in ('Unknown', ''):
        return 'Non identifié'
    if media in MEDIAS_CONNUS:
        return MEDIAS_CONNUS[media]
    media_lower = media.lower()
    for ext, region in sorted(EXTENSION_REGION.items(), key=lambda x: -len(x[0])):
        if media_lower.endswith(ext):
            return region
    return 'Non identifié'

# ============================================================
# CLOUD FUNCTION PRINCIPALE
# ============================================================

@functions_framework.http
def run_pipeline(request):

    request_args = request.args
    request_json = request.get_json(silent=True)
    
    if request_args and 'date' in request_args:
        target_date = request_args['date']
    elif request_json and 'date' in request_json:
        target_date = request_json['date']
    else:
        target_date = (datetime.utcnow() - timedelta(days=1)).strftime('%Y%m%d')
    
    print(f"Traitement des données du {target_date}")

    # etape 1 : récupérer les URLs des fichiers GDELT d'hie
    master_url = "http://data.gdeltproject.org/gdeltv2/masterfilelist.txt"
    response = requests.get(master_url, timeout=60)
    lines = response.text.strip().split('\n')

    events_urls, gkg_urls = [], []
    for line in lines:
        parts = line.strip().split(' ')
        if len(parts) == 3:
            url = parts[2]
            filename = url.split('/')[-1]
            date = filename[:8]
            if date == target_date:
                if '.export.CSV.zip' in filename:
                    events_urls.append(url)
                elif '.gkg.csv.zip' in filename:
                    gkg_urls.append(url)

    print(f"Fichiers events : {len(events_urls)} | Fichiers GKG : {len(gkg_urls)}")

    if not events_urls:
        return "Aucun fichier trouvé pour hier", 200

    # etape 2 : télécharger et filtrer events sur Bénin
    events_frames = []

    for url in events_urls:
        try:
            r = requests.get(url, timeout=30)
            z = zipfile.ZipFile(io.BytesIO(r.content))
            df = pd.read_csv(
                z.open(z.namelist()[0]),
                sep='\t', header=None,
                on_bad_lines='skip', dtype=str,
                encoding='latin-1'
            )
            # Col 53 = ActionGeo_CountryCode, Col 25 = IsRootEvent
            df_benin = df[(df[53] == 'BN') & (df[25] == '1')]
            if len(df_benin) > 0:
                events_frames.append(df_benin)
        except Exception as e:
            print(f"Erreur events {url}: {e}")
            continue

    if not events_frames:
        return f"Aucun événement Bénin trouvé pour {target_date}", 200

    df_events_raw = pd.concat(events_frames, ignore_index=True)

    # sélectionner les colonnes utiles
    cols_map = {
        0:  'GLOBALEVENTID',
        1:  'SQLDATE',
        52: 'ActionGeo_FullName',
        56: 'ActionGeo_Lat',
        57: 'ActionGeo_Long',
        34: 'AvgTone',
        33: 'NumArticles',
        32: 'NumSources',
        29: 'QuadClass',
        6:  'Actor1Name',
        16: 'Actor2Name',
        60: 'SOURCEURL',
        30: 'GoldsteinScale',
        28: 'EventRootCode',
        31: 'NumMentions',
        59: 'DATEADDED'
    }

    df_events = df_events_raw[list(cols_map.keys())].copy()
    df_events = df_events.rename(columns=cols_map)

    # dédoublonner 
    df_events = df_events.drop_duplicates(
        subset=['SQLDATE', 'SOURCEURL', 'EventRootCode', 'GoldsteinScale'],
        keep='first'
    )

    # convertir les types
    df_events['SQLDATE']        = pd.to_datetime(df_events['SQLDATE'], format='%Y%m%d', errors='coerce')
    df_events['GLOBALEVENTID']  = pd.to_numeric(df_events['GLOBALEVENTID'], errors='coerce')
    df_events['ActionGeo_Lat']  = pd.to_numeric(df_events['ActionGeo_Lat'], errors='coerce')
    df_events['ActionGeo_Long'] = pd.to_numeric(df_events['ActionGeo_Long'], errors='coerce')
    df_events['AvgTone']        = pd.to_numeric(df_events['AvgTone'], errors='coerce')
    df_events['NumArticles']    = pd.to_numeric(df_events['NumArticles'], errors='coerce')
    df_events['NumSources']     = pd.to_numeric(df_events['NumSources'], errors='coerce')
    df_events['QuadClass']      = pd.to_numeric(df_events['QuadClass'], errors='coerce')
    df_events['GoldsteinScale'] = pd.to_numeric(df_events['GoldsteinScale'], errors='coerce')
    df_events['EventRootCode']  = pd.to_numeric(df_events['EventRootCode'], errors='coerce')
    df_events['NumMentions']    = pd.to_numeric(df_events['NumMentions'], errors='coerce')
    df_events['DATEADDED']      = pd.to_numeric(df_events['DATEADDED'], errors='coerce')

    print(f"{len(df_events)} événements Bénin après dédoublonnage")

    # etape 3 télécharger GKG et filtrer sur URLs Bénin
    benin_urls_set = set(df_events['SOURCEURL'].dropna().tolist())
    gkg_frames = []

    for url in gkg_urls:
        try:
            r = requests.get(url, timeout=30)
            z = zipfile.ZipFile(io.BytesIO(r.content))
            df = pd.read_csv(
                z.open(z.namelist()[0]),
                sep='\t', header=None,
                on_bad_lines='skip', dtype=str,
                encoding='latin-1'
            )
            df_benin_gkg = df[df[4].isin(benin_urls_set)]
            if len(df_benin_gkg) > 0:
                df_benin_gkg = df_benin_gkg[[3, 4, 7]].copy()
                df_benin_gkg.columns = ['SourceCommonName', 'DocumentIdentifier', 'Themes']
                gkg_frames.append(df_benin_gkg)
        except Exception as e:
            print(f"Erreur GKG {url}: {e}")
            continue

    # etape 4 jointure events + GKG 
    if gkg_frames:
        df_gkg = pd.concat(gkg_frames, ignore_index=True).drop_duplicates('DocumentIdentifier')
        df_final = df_events.merge(
            df_gkg,
            left_on='SOURCEURL',
            right_on='DocumentIdentifier',
            how='left'
        ).drop(columns=['DocumentIdentifier'])
    else:
        df_final = df_events.copy()
        df_final['SourceCommonName'] = None
        df_final['Themes'] = None

    # 5 détection bruit Benin City
    # Créer le texte combiné
    text_cols = ['ActionGeo_FullName', 'Actor1Name', 'Actor2Name',
                 'SOURCEURL', 'Themes', 'SourceCommonName']
    df_final['combined_text'] = df_final[text_cols].fillna('').agg(' '.join, axis=1).str.lower()

    # Créer les flags
    df_final['has_benin_city'] = df_final['combined_text'].str.contains('benin city', case=False, na=False)
    df_final['has_nigeria']    = df_final['combined_text'].str.contains('nigeria', case=False, na=False)
    df_final['has_edo']        = df_final['combined_text'].str.contains(r'\bedo\b', case=False, na=False)
    df_final['has_lagos']      = df_final['combined_text'].str.contains('lagos', case=False, na=False)
    df_final['has_abuja']      = df_final['combined_text'].str.contains('abuja', case=False, na=False)

    # Les flags sont créés pour analyse mais pas utilisés comme filtre

    # supression des colonnes temporaires
    df_final = df_final.drop(columns=[
        'combined_text', 'has_benin_city', 'has_nigeria',
        'has_edo', 'has_lagos', 'has_abuja'
    ])

    # etape 6 nettoyage
    mots_nigerians = 'benin city|edo|oba of benin|benincity|lagos|nigeria'

    # Déduplication par GLOBALEVENTID
    df_final = df_final.drop_duplicates(subset='GLOBALEVENTID').copy()

    # suppression articles Benin City via SOURCEURL
    df_final = df_final[~df_final['SOURCEURL'].str.contains(mots_nigerians, case=False, na=False)]

    # Gestion des NaN
    df_final['Actor1Name']       = df_final['Actor1Name'].fillna("Unknown")
    df_final['Actor2Name']       = df_final['Actor2Name'].fillna("Unknown")
    df_final['SourceCommonName'] = df_final['SourceCommonName'].fillna("Unknown")
    df_final['Themes']           = df_final['Themes'].fillna("")

    # etape 7 : colonnes dérivées

    # definition de la variable de crise crisis
    df_final['crisis'] = df_final['QuadClass'].isin([3, 4]).astype(int)

    # domaine
    df_final['Domaine'] = df_final['Themes'].apply(extraire_domaine)

    # Region
    df_final['Region'] = df_final['SourceCommonName'].apply(associer_region)

    # étape 8  : colonnes finales
    colonnes_finales = [
        'GLOBALEVENTID', 'SQLDATE', 'ActionGeo_FullName',
        'ActionGeo_Lat', 'ActionGeo_Long', 'AvgTone',
        'NumArticles', 'NumSources', 'QuadClass',
        'Actor1Name', 'Actor2Name', 'SOURCEURL',
        'GoldsteinScale', 'EventRootCode', 'NumMentions',
        'DATEADDED', 'Themes', 'SourceCommonName',
        'Domaine', 'Region', 'crisis'
    ]
    df_final = df_final[colonnes_finales]

    print(f"{len(df_final)} lignes à charger dans BigQuery")

     # étape 9 supprimer les données existantes pour cette date 
    client = bigquery.Client()
    table_id = "isheero.test.benin_final"
    
    delete_query = f"""
        DELETE FROM `{table_id}`
        WHERE CAST(SQLDATE AS STRING) = '{target_date[:4]}-{target_date[4:6]}-{target_date[6:8]}'
    """
    client.query(delete_query).result()
    print(f"Données existantes supprimées pour {target_date}")

    # etape 10 : charger dans BigQuery en mode APPEND 

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        autodetect=False,
        schema=[
            bigquery.SchemaField("GLOBALEVENTID",      "INTEGER"),
            bigquery.SchemaField("SQLDATE",            "DATE"),
            bigquery.SchemaField("ActionGeo_FullName", "STRING"),
            bigquery.SchemaField("ActionGeo_Lat",      "FLOAT"),
            bigquery.SchemaField("ActionGeo_Long",     "FLOAT"),
            bigquery.SchemaField("AvgTone",            "FLOAT"),
            bigquery.SchemaField("NumArticles",        "INTEGER"),
            bigquery.SchemaField("NumSources",         "INTEGER"),
            bigquery.SchemaField("QuadClass",          "INTEGER"),
            bigquery.SchemaField("Actor1Name",         "STRING"),
            bigquery.SchemaField("Actor2Name",         "STRING"),
            bigquery.SchemaField("SOURCEURL",          "STRING"),
            bigquery.SchemaField("GoldsteinScale",     "FLOAT"),
            bigquery.SchemaField("EventRootCode",      "INTEGER"),
            bigquery.SchemaField("NumMentions",        "INTEGER"),
            bigquery.SchemaField("DATEADDED",          "INTEGER"),
            bigquery.SchemaField("Themes",             "STRING"),
            bigquery.SchemaField("SourceCommonName",   "STRING"),
            bigquery.SchemaField("Domaine",            "STRING"),
            bigquery.SchemaField("Region",             "STRING"),
            bigquery.SchemaField("crisis",             "INTEGER"),
        ]
    )

    job = client.load_table_from_dataframe(df_final, table_id, job_config=job_config)
    job.result()

    msg = f"Pipeline exécuté : {len(df_final)} lignes ajoutées à {table_id}"
    print(msg)
    return msg, 200
