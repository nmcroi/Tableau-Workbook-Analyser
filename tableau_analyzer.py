from lxml import etree as ET
import zipfile
import json
import os
import shutil
import sys
from datetime import datetime

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
        print(f"INFO: Geregistreerde namespaces: {list(NAMESPACES.keys())}")
    except ET.ParseError as e:
        print(f"FOUT: XML Parse Fout bij registratie namespaces in {os.path.basename(bestands_pad)}: {e}")
    except Exception as e:
        print(f"FOUT: Algemene fout bij registratie namespaces: {e}")


def extraheer_twb_uit_twbx(twbx_bestands_pad, tijdelijke_map):
    if not os.path.exists(tijdelijke_map):
        os.makedirs(tijdelijke_map)
    try:
        with zipfile.ZipFile(twbx_bestands_pad, 'r') as zip_ref:
            twb_files = [name for name in zip_ref.namelist() if name.endswith('.twb')]
            if not twb_files:
                print(f"FOUT: Geen .twb bestand gevonden in {twbx_bestands_pad}")
                return None
            twb_file_in_zip = twb_files[0]
            # Voorkeur voor .twb in root van zip
            for f_name in twb_files:
                if '/' not in f_name and '\\' not in f_name:
                    twb_file_in_zip = f_name
                    break
            doel_pad = os.path.join(tijdelijke_map, os.path.basename(twb_file_in_zip))
            with zip_ref.open(twb_file_in_zip) as source, open(doel_pad, 'wb') as target:
                shutil.copyfileobj(source, target)
            print(f"INFO: .twb bestand geëxtraheerd naar: {doel_pad}")
            return doel_pad
    except Exception as e:
        print(f"FOUT bij uitpakken {twbx_bestands_pad}: {e}")
        return None

def analyseer_tableau_bestand(twb_bestands_pad):
    """
    Analyseert een .twb-bestand en extraheert metadata.
    Args:
        twb_bestands_pad (str): Het pad naar het .twb-bestand.
    Returns:
        dict: Een dictionary met de geëxtraheerde metadata, of None bij een fout.
    """
    print(f"\nINFO: Start gedetailleerde analyse van: {os.path.basename(twb_bestands_pad)}")
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

    except ET.ParseError as e:
        print(f"FOUT: XML Parse Fout in {os.path.basename(twb_bestands_pad)}: {e}")
        return None
    except Exception as e:
        print(f"FOUT: Algemene fout tijdens analyse van {os.path.basename(twb_bestands_pad)}: {e}")
        import traceback
        traceback.print_exc() # Voor meer details bij onverwachte fouten
        return None

    print(f"INFO: Gedetailleerde analyse van {os.path.basename(twb_bestands_pad)} voltooid.")
    return project_data

def sla_op_als_json(data, uitvoer_bestands_pad):
    """Slaat de geëxtraheerde data op als een JSON-bestand."""
    try:
        with open(uitvoer_bestands_pad, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=4, ensure_ascii=False)
        print(f"\nINFO: Analyse succesvol opgeslagen als: {uitvoer_bestands_pad}")
        return True
    except IOError as e:
        print(f"\nFOUT: Kon JSON-bestand niet schrijven naar {uitvoer_bestands_pad}: {e}")
        return False
    except Exception as e:
        print(f"\nFOUT: Algemene fout bij opslaan JSON: {e}")
        return False

def process_tableau_file(file_path):
    """Verwerkt een .twb of .twbx bestand."""
    print(f"\nVerwerken bestand: {file_path}")
    
    is_twbx = file_path.lower().endswith('.twbx')
    twb_to_analyze = file_path
    temp_dir_for_twbx = None

    if is_twbx:
        print("INFO: .twbx bestand gedetecteerd, bezig met uitpakken...")
        temp_dir_for_twbx = "temp_tableau_extract_" + os.path.basename(file_path).replace('.', '_')
        extracted_twb = extraheer_twb_uit_twbx(file_path, temp_dir_for_twbx)
        if not extracted_twb:
            return False # Fout tijdens extractie
        twb_to_analyze = extracted_twb
    
    # Voer hier de gedetailleerde analyse uit
    analyse_data = analyseer_tableau_bestand(twb_to_analyze)
    analysis_successful = False # Standaard

    if analyse_data:
        # Bepaal JSON output pad
        base_name = os.path.basename(file_path)
        output_json_name = os.path.splitext(base_name)[0] + "_analyse.json"
        # Gebruik de globale SCRIPT_DIR
        output_json_pad = os.path.join(SCRIPT_DIR, output_json_name)
        
        if sla_op_als_json(analyse_data, output_json_pad):
            analysis_successful = True
        else:
            print("FOUT: Opslaan als JSON mislukt.")
            analysis_successful = False # Expliciet voor duidelijkheid
    else:
        print("FOUT: Analyse heeft geen data geretourneerd.")
        analysis_successful = False

    # Ruim tijdelijke map op indien gebruikt voor .twbx
    if temp_dir_for_twbx and os.path.exists(temp_dir_for_twbx):
        try:
            shutil.rmtree(temp_dir_for_twbx)
            print(f"INFO: Tijdelijke map {temp_dir_for_twbx} opgeruimd.")
        except Exception as e:
            print(f"WAARSCHUWING: Kon tijdelijke map {temp_dir_for_twbx} niet opruimen: {e}")
            
    return analysis_successful

def main():
    print("Tableau Analyzer - Vereenvoudigde versie gestart")
    print(f"Python versie: {sys.version}")
    print(f"Werkmap: {os.getcwd()}")
    
    if len(sys.argv) < 2:
        print("\nFOUT: Geen bestand opgegeven.")
        print("Gebruik: python3 tableau_analyzer.py <pad_naar_bestand.twb_of_twbx>")
        return 1
        
    target_file = os.path.abspath(sys.argv[1].strip('"\' '))
    print(f"Doelbestand: {target_file}")

    if not os.path.exists(target_file):
        print(f"FOUT: Bestand niet gevonden: {target_file}")
        return 1
        
    if not (target_file.lower().endswith('.twb') or target_file.lower().endswith('.twbx')):
        print("FOUT: Ongeldig bestandstype. Alleen .twb of .twbx bestanden worden ondersteund.")
        return 1

    print("\n" + "="*50)
    if process_tableau_file(target_file):
        print("\n" + "="*50)
        print("Verwerking succesvol afgerond.")
        return 0
    else:
        print("\n" + "="*50)
        print("Verwerking mislukt.")
        return 1

if __name__ == "__main__":
    sys.exit(main())
