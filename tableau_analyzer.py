from lxml import etree as ET
import zipfile
import json
import os
import shutil
import sys
from datetime import datetime
import logging

# Logging setup
logger = logging.getLogger(__name__)
# Configure logger (do this once, preferably at application entry point or module import)
# For now, basic configuration. This could be moved to main() or a dedicated setup function.
if not logger.handlers: # Avoid adding multiple handlers if the module is reloaded
    handler = logging.StreamHandler(sys.stderr)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(name)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO) # Default level, can be changed as needed

# Fallback voor __file__ in geval van directe uitvoering of interactieve sessies
try:
    SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
except NameError:
    SCRIPT_DIR = os.getcwd()

# Constanten voor namespaces
NAMESPACES = {
    'user': 'http://www.tableausoftware.com/xml/user',
    # Voeg hier eventueel andere vaak gebruikte namespaces toe
}

# Functies voor complexiteit en afhankelijkheden
def score_complexity(formula_string):
    """Scoort de complexiteit van een Tableau formule."""
    if not formula_string:
        return "Onbekend" # Of "N/A"

    length = len(formula_string)
    # Ruwe telling van functies (vereenvoudigd: telt open haakjes)
    # Een betere methode zou zijn om specifieke Tableau functienamen te tellen.
    num_functions = formula_string.count('(') 
    
    # Basis nesting check (vereenvoudigd: diepte van haakjes)
    # Dit is een zeer basale indicator en kan verfijnd worden.
    nesting_depth = 0
    max_nesting_depth = 0
    for char in formula_string:
        if char == '(':
            nesting_depth += 1
            max_nesting_depth = max(max_nesting_depth, nesting_depth)
        elif char == ')':
            nesting_depth -= 1

    if length >= 150 or num_functions > 3 or max_nesting_depth > 2: # Aangepast voor max_nesting_depth
        return "Complex"
    elif length >= 50 or num_functions > 1 or max_nesting_depth > 1: # Aangepast voor max_nesting_depth
        return "Gemiddeld"
    else:
        return "Eenvoudig"

def extract_field_dependencies(formula_string, all_fields):
    """Extraheert veldafhankelijkheden uit een formule."""
    dependencies = set() # Gebruik een set om duplicaten te voorkomen
    if not formula_string or not all_fields:
        return []

    # Zoek naar velden tussen blokhaken, bijv. [Sales] of [Order Date]
    # Deze regex probeert ook veldnamen met spaties correct te matchen
    import re
    # ([^\[\]]+) zorgt ervoor dat we de inhoud binnen de haken krijgen
    # We moeten de blokhaken escapen omdat ze speciale betekenis hebben in regex.
    potential_fields = re.findall(r'\[([^\[\]]+)\]', formula_string)
    
    for pf in potential_fields:
        # Vergelijk met de lijst van alle bekende velden (case-insensitive voor robuustheid)
        # Soms hebben velden in formules een prefix van hun databron, bv [DatasourceName].[FieldName]
        # Voor nu negeren we de datasource prefix in de matching, maar dit kan verfijnd worden.
        cleaned_pf = pf.split('.')[-1] # Neem het laatste deel als er een punt is
        
        for field_name in all_fields:
            if field_name.lower() == cleaned_pf.lower():
                dependencies.add(field_name) # Voeg de originele veldnaam toe (met juiste casing)
                break 
                # Als een veldnaam in de formule [Orders (Sample)].[Order ID] is
                # en all_fields bevat "Order ID", dan moet dit matchen.
                # De huidige cleaned_pf logica zal "Order ID]" teruggeven zonder de laatste ']'
                # Dit moet nog verfijnd worden als dit een probleem blijkt.
                # Voor nu, uitgaande van simpele [FieldName] of [FieldName met spaties]

    return list(dependencies)

def registreer_alle_namespaces(bestands_pad):
    """
    Parseert het XML-bestand en registreert alle gevonden namespaces.
    Dit is cruciaal voor ElementTree om elementen correct te vinden.
    """
    try:
        # Gebruik een set om dubbele registraties van dezelfde URI onder verschillende prefixen te voorkomen
        # Hoewel ET.register_namespace dit zelf ook zou moeten afhandelen.
        geziene_uris = set()
        for event, elem in ET.iterparse(bestands_pad, events=('start-ns',)):
            ns_prefix, ns_uri = elem
            if ns_uri not in geziene_uris:
                ET.register_namespace(ns_prefix, ns_uri)
                NAMESPACES[ns_prefix] = ns_uri # Voeg toe aan onze globale dictionary
                geziene_uris.add(ns_uri)
        logger.info(f"Geregistreerde namespaces: {list(NAMESPACES.keys())}")
    except ET.ParseError as e:
        logger.error(f"XML Parse Fout bij registratie namespaces in {os.path.basename(bestands_pad)}: {e}")
        # Optioneel: raise de error opnieuw als de caller dit moet weten
        # raise
    except Exception as e:
        logger.exception(f"Algemene fout bij registratie namespaces voor {os.path.basename(bestands_pad)}: ")
        # Optioneel: raise


def extraheer_twb_uit_twbx(twbx_bestands_pad, tijdelijke_map):
    try:
        if not os.path.exists(tijdelijke_map):
            os.makedirs(tijdelijke_map)
            logger.info(f"Tijdelijke map aangemaakt: {tijdelijke_map}")

        with zipfile.ZipFile(twbx_bestands_pad, 'r') as zip_ref:
            twb_files = [name for name in zip_ref.namelist() if name.endswith('.twb')]
            if not twb_files:
                logger.error(f"Geen .twb bestand gevonden in {twbx_bestands_pad}")
                return None
            
            twb_file_in_zip = twb_files[0] # Standaard de eerste
            # Voorkeur voor .twb in root van zip
            for f_name in twb_files:
                if '/' not in f_name and '\\' not in f_name:
                    twb_file_in_zip = f_name
                    break
            
            logger.info(f"Geselecteerd .twb bestand uit archief: {twb_file_in_zip}")
            doel_pad = os.path.join(tijdelijke_map, os.path.basename(twb_file_in_zip))
            
            with zip_ref.open(twb_file_in_zip) as source, open(doel_pad, 'wb') as target:
                shutil.copyfileobj(source, target)
            logger.info(f".twb bestand geëxtraheerd naar: {doel_pad}")
            return doel_pad
            
    except FileNotFoundError:
        logger.error(f"Bestand niet gevonden: {twbx_bestands_pad}")
        raise # Re-raise voor app.py om specifiek te handelen
    except zipfile.BadZipFile:
        logger.error(f"Ongeldig of corrupt zip-archief: {twbx_bestands_pad}")
        raise # Re-raise voor app.py
    except (KeyError, IndexError) as e:
        logger.error(f"Fout bij vinden van .twb in archief {twbx_bestands_pad}: {e}")
        # Dit kan duiden op een onverwachte structuur of geen .twb
        raise # Re-raise met een eigen gedefinieerde exception zou nog beter zijn
    except Exception as e:
        logger.exception(f"Algemene fout bij uitpakken van {twbx_bestands_pad}: ")
        # Vang andere onverwachte fouten op
        raise # Re-raise voor generieke afhandeling

def analyseer_tableau_bestand(twb_bestands_pad):
    """
    Analyseert een .twb-bestand en extraheert metadata.
    Args:
        twb_bestands_pad (str): Het pad naar het .twb-bestand.
    Returns:
        dict: Een dictionary met de geëxtraheerde metadata, of None bij een fout.
    """
    logger.info(f"Start gedetailleerde analyse van: {os.path.basename(twb_bestands_pad)}")
    project_data = {
        "bestandsnaam": os.path.basename(twb_bestands_pad),
        "extract_datum": datetime.now().isoformat(),
        "databronnen": [],
        "werkbladen": [],
        "dashboards": [],
        "verhalen": [],
        "berekende_velden": [],
        "parameters": [],
        "extensies": []
    }

    try:
        registreer_alle_namespaces(twb_bestands_pad) # Essentieel voor correcte XPath queries
        tree = ET.parse(twb_bestands_pad)
        root = tree.getroot()

        # 1. Databronnen
        for ds_node in root.findall('.//datasource', namespaces=NAMESPACES):
            ds_info = {
                "naam": ds_node.get('name', ds_node.get('caption', 'Onbekende Databron')),
                "versie": ds_node.get('version', 'N/A'),
                "verbindingen": [],
                "kolommen": []
            }
            for conn_node in ds_node.findall('.//connection', namespaces=NAMESPACES):
                conn_info = {
                    "class": conn_node.get('class'),
                    "dbname": conn_node.get('dbname'),
                    "server": conn_node.get('server'),
                    "username": conn_node.get('username'),
                    # Voeg meer attributen toe indien nodig
                }
                ds_info["verbindingen"].append(conn_info)
            
            for col_node in ds_node.findall('.//column', namespaces=NAMESPACES):
                col_data = {
                    "naam": col_node.get('name'),
                    "alias": col_node.get('alias'),
                    "datatype": col_node.get('datatype'),
                    "rol": col_node.get('role'), # dimension, measure
                    "type": col_node.get('type'), # nominal, quantitative, ordinal, temporal
                    "caption": col_node.get('caption')
                }
                calculation_node = col_node.find('.//calculation', namespaces=NAMESPACES)
                if calculation_node is not None:
                    col_data["is_berekend_veld"] = True
                    col_data["formule"] = calculation_node.get('formula', '').strip()
                else:
                    col_data["is_berekend_veld"] = False
                ds_info["kolommen"].append(col_data)
            project_data["databronnen"].append(ds_info)

        # 2. Werkbladen
        for ws_node in root.findall('.//worksheet', namespaces=NAMESPACES):
            ws_info = {
                "naam": ws_node.get('name', 'Onbekend Werkblad'),
                "gebruikte_databronnen": [],
                "gebruikte_velden_direct": [],
                "filters": []
            }
            for dep_node in ws_node.findall('.//datasource-dependencies', namespaces=NAMESPACES):
                ds_name = dep_node.get('datasource')
                if ds_name:
                    ws_info["gebruikte_databronnen"].append(ds_name)
            # Velden gebruikt (vereenvoudigd)
            for field_node in ws_node.findall('.//datasource-dependencies/column', namespaces=NAMESPACES):
                ws_info["gebruikte_velden_direct"].append(field_node.get('name'))
            project_data["werkbladen"].append(ws_info)

        # 3. Dashboards
        for dash_node in root.findall('.//dashboard', namespaces=NAMESPACES):
            dash_info = {
                "naam": dash_node.get('name', 'Onbekend Dashboard'),
                "objecten": []
            }
            for zone_node in dash_node.findall('.//zone', namespaces=NAMESPACES):
                obj_info = {
                    "id": zone_node.get('id'),
                    "type": zone_node.get('type-v2'), 
                    "naam_object": zone_node.get('name'), # Vaak naam van werkblad
                }
                dash_info["objecten"].append(obj_info)
            project_data["dashboards"].append(dash_info)
        
        # (Voeg hier later extractie voor Verhalen, Parameters, Extensies toe indien nodig)

        # Na het verzamelen van alle kolommen, bepaal afhankelijkheden voor berekende velden
        all_field_names = []
        for ds in project_data["databronnen"]:
            for col in ds["kolommen"]:
                # Gebruik caption als die er is, anders naam. Dit moet consistent zijn met hoe velden in formules worden gerefereerd.
                # Tableau gebruikt meestal de 'caption' of de 'name' (vaak [name]) in formules.
                # We nemen aan dat de 'name' (interne naam) het meest betrouwbaar is voor matching.
                # Velden in formules worden meestal gerefereerd als [Veldnaam] wat overeenkomt met 'name' of 'caption'.
                # Voor nu gebruiken we 'name' als de primaire identificatie.
                if col.get("naam"): # Zorg ervoor dat er een naam is
                    all_field_names.append(col["naam"])
        
        # Verwijder duplicaten als veldnamen (zonder datasource prefix) in meerdere databronnen voorkomen
        # Dit is een vereenvoudiging; echte afhankelijkheden kunnen datasource-specifiek zijn.
        unique_field_names = list(set(all_field_names))
        
        for ds in project_data["databronnen"]:
            for col_data in ds["kolommen"]:
                if col_data.get("is_berekend_veld") and col_data.get("formule"):
                    formula = col_data["formule"]
                    col_data["complexiteit"] = score_complexity(formula)
                    col_data["afhankelijkheden"] = extract_field_dependencies(formula, unique_field_names)
                elif col_data.get("is_berekend_veld"): # Berekend veld maar geen formule? Geef standaard waarden.
                    col_data["complexiteit"] = "Onbekend"
                    col_data["afhankelijkheden"] = []

    except ET.ParseError as e:
        logger.error(f"XML Parse Fout in {os.path.basename(twb_bestands_pad)}: {e}")
        # Stuur de error door zodat app.py deze kan afhandelen
        raise
    except Exception as e:
        # Vang andere onverwachte fouten tijdens de analyse op
        logger.exception(f"Algemene fout tijdens analyse van {os.path.basename(twb_bestands_pad)}: ")
        raise # Stuur door voor generieke afhandeling in app.py

    logger.info(f"Gedetailleerde analyse van {os.path.basename(twb_bestands_pad)} voltooid.")
    return project_data

def sla_op_als_json(data, uitvoer_bestands_pad):
    """Slaat de geëxtraheerde data op als een JSON-bestand."""
    try:
        with open(uitvoer_bestands_pad, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=4, ensure_ascii=False)
        logger.info(f"Analyse succesvol opgeslagen als: {uitvoer_bestands_pad}")
        return True
    except (IOError, PermissionError, FileNotFoundError) as e: # Meer specifieke IO errors
        logger.error(f"Kon JSON-bestand niet schrijven naar {uitvoer_bestands_pad}: {e}")
        return False
    except TypeError as e: # Voor niet-serialiseerbare data
        logger.error(f"Data is niet JSON serialiseerbaar bij opslaan naar {uitvoer_bestands_pad}: {e}")
        return False
    except Exception as e:
        logger.exception(f"Algemene fout bij opslaan JSON naar {uitvoer_bestands_pad}: ")
        return False

def process_tableau_file(file_path):
    """Verwerkt een .twb of .twbx bestand."""
    logger.info(f"Start verwerking bestand: {file_path}")
    
    is_twbx = file_path.lower().endswith('.twbx')
    twb_to_analyze = file_path
    temp_dir_for_twbx = None
    analysis_successful = False 

    try:
        if is_twbx:
            logger.info(".twbx bestand gedetecteerd, bezig met uitpakken...")
            # Genereer een uniekere tijdelijke mapnaam om conflicten te vermijden
            base_name = os.path.basename(file_path).replace('.', '_')
            timestamp = datetime.now().strftime("%Y%m%d%H%M%S%f")
            temp_dir_for_twbx = f"temp_tableau_extract_{base_name}_{timestamp}"
            
            extracted_twb = extraheer_twb_uit_twbx(file_path, temp_dir_for_twbx)
            # extraheer_twb_uit_twbx zal nu exceptions raisen, die hieronder worden gevangen
            twb_to_analyze = extracted_twb
        
        analyse_data = analyseer_tableau_bestand(twb_to_analyze)
        # analyseer_tableau_bestand zal nu exceptions raisen
        
        # Bepaal JSON output pad
        # Sla op in dezelfde map als het script, tenzij anders geconfigureerd
        output_dir = SCRIPT_DIR 
        # output_dir = "." # Of een andere configureerbare map
        # os.makedirs(output_dir, exist_ok=True) # Zorg ervoor dat de output map bestaat

        base_name_original_file = os.path.basename(file_path) # Gebruik originele bestandsnaam voor output
        output_json_name = os.path.splitext(base_name_original_file)[0] + "_analyse.json"
        output_json_pad = os.path.join(output_dir, output_json_name)
        
        if sla_op_als_json(analyse_data, output_json_pad):
            analysis_successful = True
        else:
            # sla_op_als_json logt zelf al de fout
            analysis_successful = False

    except (FileNotFoundError, zipfile.BadZipFile, ET.ParseError, KeyError, IndexError) as e:
        # Deze errors zijn al gelogd in de specifiekere functies en worden hier opnieuw geraised
        # zodat app.py ze kan tonen aan de gebruiker.
        # Voor CLI gebruik, kunnen we ze hier ook loggen als dat gewenst is, maar het zou dubbel zijn.
        logger.error(f"Specifieke fout tijdens verwerking van {file_path}: {type(e).__name__} - {e}")
        # Geen 'raise' hier, want process_tableau_file moet True/False retourneren voor de CLI main.
        analysis_successful = False # Zorg ervoor dat het False is
    except Exception as e:
        # Vang alle andere onverwachte exceptions die mogelijk niet door de lagere functies zijn geraised/gelogd.
        logger.exception(f"Onverwachte algemene fout tijdens verwerking van {file_path}: ")
        analysis_successful = False
    finally:
        if temp_dir_for_twbx and os.path.exists(temp_dir_for_twbx):
            try:
                shutil.rmtree(temp_dir_for_twbx)
                logger.info(f"Tijdelijke map {temp_dir_for_twbx} opgeruimd.")
            except Exception as e:
                logger.warning(f"Kon tijdelijke map {temp_dir_for_twbx} niet opruimen: {e}")
            
    return analysis_successful

def main():
    # Configureer logger voor CLI gebruik
    # Als dit script als library wordt geïmporteerd, wil je dit misschien niet hier doen.
    logging.basicConfig(level=logging.INFO, 
                        format='%(asctime)s - %(levelname)s - %(name)s - %(message)s',
                        handlers=[logging.StreamHandler(sys.stderr)])
    
    logger.info("Tableau Analyzer CLI gestart")
    logger.info(f"Python versie: {sys.version}")
    logger.info(f"Werkmap: {os.getcwd()}")
    
    if len(sys.argv) < 2:
        # Fallback: als dit script per abuis als Streamlit main file wordt gestart (zoals op Streamlit Cloud),
        # start dan de echte Streamlit UI uit app.py in plaats van te stoppen met een foutmelding.
        try:
            import streamlit as st  # type: ignore
            logger.info("Geen CLI-bestand opgegeven; start Streamlit UI vanuit app.py als fallback.")
            from app import load_custom_css as _load_css, main as _app_main  # lazy import om import-cycli te vermijden
            _load_css()
            _app_main()
            return 0
        except Exception:
            # Als we de Streamlit UI niet kunnen starten, val terug op de originele CLI melding
            logger.error("Geen bestand opgegeven.")
            logger.info("Gebruik: python3 tableau_analyzer.py <pad_naar_bestand.twb_of_twbx>")
            return 1
        
    target_file = os.path.abspath(sys.argv[1].strip('"\' '))
    logger.info(f"Doelbestand: {target_file}")

    if not os.path.exists(target_file):
        logger.error(f"Bestand niet gevonden: {target_file}")
        return 1
        
    if not (target_file.lower().endswith('.twb') or target_file.lower().endswith('.twbx')):
        logger.error("Ongeldig bestandstype. Alleen .twb of .twbx bestanden worden ondersteund.")
        return 1

    logger.info("="*50)
    if process_tableau_file(target_file):
        logger.info("="*50)
        logger.info("Verwerking succesvol afgerond.")
        return 0
    else:
        logger.info("="*50)
        logger.error("Verwerking mislukt. Zie logs hierboven voor details.")
        return 1

if __name__ == "__main__":
    # Verplaats logger configuratie naar main om dubbele handlers te voorkomen als module wordt geladen
    # De globale logger setup bovenaan het bestand zal gebruikt worden als dit als library wordt geïmporteerd.
    # Voor CLI, kan main() de configuratie overschrijven of verfijnen.
    # In dit geval, de basicConfig in main() zal de root logger configureren.
    # De logger = logging.getLogger(__name__) zal deze configuratie erven.
    sys.exit(main()) 