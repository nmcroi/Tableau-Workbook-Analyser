import streamlit as st
import os
import json
import pandas as pd
import io
import zipfile
from datetime import datetime
from lxml import etree as ET
from tableau_analyzer import process_tableau_file, analyseer_tableau_bestand, extraheer_twb_uit_twbx, sla_op_als_json, NAMESPACES

# Vertaaltabellen voor technische termen naar begrijpelijke taal
DATATYPE_TRANSLATION = {
    'string': 'Tekst',
    'integer': 'Geheel getal',
    'real': 'Decimaal getal',
    'boolean': 'Waar/Onwaar',
    'date': 'Datum',
    'datetime': 'Datum en tijd',
    'spatial': 'Geografische data',
    'table': 'Tabel',
    'unknown': 'Onbekend type'
}

ROLE_TRANSLATION = {
    'dimension': 'Dimensie',
    'measure': 'Maat',
    'unknown': 'Onbekend'
}

TYPE_TRANSLATION = {
    'nominal': 'Categorie',
    'quantitative': 'Kwantitatief',
    'ordinal': 'Rangschikking',
    'temporal': 'Tijd',
    'unknown': 'Onbekend'
}

def translate_datatype(dt):
    return DATATYPE_TRANSLATION.get(dt, dt)

def translate_role(role):
    return ROLE_TRANSLATION.get(role, role)

def translate_type(t):
    return TYPE_TRANSLATION.get(t, t)

# Pagina configuratie
st.set_page_config(
    page_title="Tableau Analyzer",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Functie om aangepaste CSS te laden
def load_custom_css():
    """Laadt aangepaste CSS uit static/style.css en past deze toe."""
    css_file_path = os.path.join("static", "style.css")
    if os.path.exists(css_file_path):
        with open(css_file_path, "r") as f:
            css_content = f.read()
        st.markdown(f'<style>{css_content}</style>', unsafe_allow_html=True)
    else:
        st.warning("Aangepast CSS-bestand (style.css) niet gevonden in de map 'static'.")

def display_field_info(field):
    """Toon veldinformatie in een leesbaar formaat"""
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Naam", field.get('naam', 'Onbekend'))
    with col2:
        st.metric("Type", translate_datatype(field.get('datatype', 'unknown')))
    with col3:
        st.metric("Rol", translate_role(field.get('rol', 'unknown')))
    
    # Toon aanvullende informatie
    if 'formule' in field and field['formule']:
        with st.expander("Details berekend veld"):
            st.write("**Formule:**")
            st.code(field['formule'], language='sql') # 'tableau' of 'markdown' indien beter
            
            if 'complexiteit' in field:
                st.write(f"**Complexiteit:** {field['complexiteit']}")
            
            if 'afhankelijkheden' in field:
                st.write("**Afhankelijkheden:**")
                if field['afhankelijkheden']:
                    for dep in field['afhankelijkheden']:
                        st.write(f"- `{dep}`") # Gebruik code formatting voor veldnamen
                else:
                    st.write("Geen directe afhankelijkheden gevonden.")

    if 'alias' in field and field['alias']:
        st.caption(f"Weergavenaam (Alias): {field['alias']}")

def display_datasource(ds):
    """Toon informatie over een databron"""
    st.subheader(f"Databron: {ds.get('naam', 'Onbekend')}")
    
    # Toon verbindingen
    if 'verbindingen' in ds and ds['verbindingen']:
        with st.expander(f"🔌 {len(ds['verbindingen'])} verbinding(en)"):
            for conn in ds['verbindingen']:
                st.write(f"- **Type:** {conn.get('class', 'Onbekend')}")
                if conn.get('server'):
                    st.write(f"  - Server: `{conn['server']}`")
                if conn.get('dbname'):
                    st.write(f"  - Database: `{conn['dbname']}`")
    
    # Toon kolommen/velden
    if 'kolommen' in ds and ds['kolommen']:
        st.subheader("Velden")
        for field in ds['kolommen']:
            with st.container():
                display_field_info(field)
                st.markdown("---")

def display_worksheet(ws):
    """Toon informatie over een werkblad"""
    st.subheader(f"Werkblad: {ws.get('naam', 'Onbekend')}")
    
    # Toon gebruikte databronnen
    if 'gebruikte_databronnen' in ws and ws['gebruikte_databronnen']:
        st.write("**Gebruikte databronnen:**")
        for bron in ws['gebruikte_databronnen']:
            st.write(f"- {bron}")
    
    # Toon gebruikte velden
    if 'gebruikte_velden_direct' in ws and ws['gebruikte_velden_direct']:
        with st.expander(f"🔍 {len(ws['gebruikte_velden_direct'])} gebruikte velden"):
            for veld in ws['gebruikte_velden_direct']:
                st.write(f"- {veld}")

def display_dashboard(db):
    """Toon informatie over een dashboard"""
    st.subheader(f"Dashboard: {db.get('naam', 'Onbekend')}")
    
    # Toon dashboard objecten
    if 'objecten' in db and db['objecten']:
        st.write("**Onderdelen:**")
        for obj in db['objecten']:
            obj_type = obj.get('type', 'onbekend')
            obj_name = obj.get('naam_object', 'Zonder naam')
            st.write(f"- **{obj_type.capitalize()}:** {obj_name}")

def main():
    # Header section with logos and title
    col1, col2, col3 = st.columns([1.5, 0.5, 3], gap="small")

    with col1:
        st.image("static/images/ghx_logo.png", width=200)
        # The diagnostic HTML img tag has been removed from here.

    with col2:
        st.image("static/images/tableau_logo.png", width=60)

    with col3:
        st.title("Tableau Workbook Analyzer")
        st.caption("Analyseer en optimaliseer je Tableau werkboeken")
    
    st.markdown("---") # Separator

    # Original markdown for upload instruction (re-added)
    st.markdown("""
    Upload een Tableau bestand (.twb of .twbx) om de structuur en metadata te analyseren. 
    Je kunt ook aanvullende bestanden uploaden voor extra context.
    """)
    
    # Tabbladen voor verschillende soorten bestanden
    tab1, tab2 = st.tabs(["Tableau Bestand", "Extra Bestanden"])
    
    with tab1:
        st.subheader("Tableau Bestand")
        uploaded_file = st.file_uploader("Kies een Tableau bestand (.twb of .twbx)", 
                                     type=['twb', 'twbx'],
                                     key="tableau_uploader_1")
    
    with tab2:
        st.subheader("Extra Bestanden")
        st.write("Voeg hier extra bestanden toe voor aanvullende context (optioneel):")
        
        # Uploadvelden voor verschillende bestandstypen
        sql_files = st.file_uploader("SQL Bestanden", 
                                   type=['sql'], 
                                   accept_multiple_files=True,
                                   help="Upload SQL bestanden voor analyse van queries")
        
        excel_files = st.file_uploader("Excel Bestanden", 
                                     type=['xlsx', 'xls'], 
                                     accept_multiple_files=True,
                                     help="Upload Excel bestanden voor aanvullende data")
        
        csv_files = st.file_uploader("CSV Bestanden", 
                                   type=['csv'], 
                                   accept_multiple_files=True,
                                   help="Upload CSV bestanden voor aanvullende data")
        
        # Toon voorbeeld van geüploade bestanden
        if sql_files or excel_files or csv_files:
            st.subheader("Geüploade bestanden")
            
            if sql_files:
                st.write("**SQL Bestanden:**")
                for file in sql_files:
                    st.write(f"- {file.name} ({file.size/1024:.1f} KB)")
            
            if excel_files:
                st.write("**Excel Bestanden:**")
                for file in excel_files:
                    st.write(f"- {file.name} ({file.size/1024:.1f} KB)")
            
            if csv_files:
                st.write("**CSV Bestanden:**")
                for file in csv_files:
                    st.write(f"- {file.name} ({file.size/1024:.1f} KB)")
    
    if uploaded_file is not None:
        # Toon bestandsinformatie in de sidebar
        with st.sidebar:
            st.subheader("Bestandsinformatie")
            st.write(f"**Naam:** {uploaded_file.name}")
            st.write(f"**Grootte:** {uploaded_file.size / 1024:.2f} KB")
        
        # Analyseer knop
        if st.button("Analyseer bestand", type="primary", key="analyze_btn"):
            # Tijdelijke map aanmaken
            temp_dir = "temp_upload"
            os.makedirs(temp_dir, exist_ok=True)
            temp_file = os.path.join(temp_dir, uploaded_file.name)
            
            # Bestand tijdelijk opslaan
            with open(temp_file, "wb") as f:
                f.write(uploaded_file.getbuffer())
            
            # Analyse uitvoeren
            with st.spinner("Bezig met analyseren... Dit kan even duren voor grote bestanden."):
                analysis_path = temp_file
                temp_dir_for_twbx_extraction = None # Voor opruimen
                try:
                    if uploaded_file.name.lower().endswith('.twbx'):
                        st.info("Het is een .twbx bestand, bezig met uitpakken...")
                        # Maak een unieke submap voor extractie binnen temp_dir
                        # Dit voorkomt conflicten als bestandsnamen in zip hetzelfde zijn als de zip zelf.
                        timestamp = datetime.now().strftime("%Y%m%d%H%M%S%f")
                        temp_dir_for_twbx_extraction = os.path.join(temp_dir, f"extracted_{timestamp}")
                        os.makedirs(temp_dir_for_twbx_extraction, exist_ok=True)
                        
                        # extraheer_twb_uit_twbx is nu onderdeel van tableau_analyzer en logt zelf
                        # en raised exceptions bij fouten.
                        analysis_path = extraheer_twb_uit_twbx(temp_file, temp_dir_for_twbx_extraction)
                        if not analysis_path: 
                            # Dit zou niet moeten gebeuren als extraheer_twb_uit_twbx een exception raised.
                            # Maar als het None retourneert zonder exception:
                            st.error("Kon het .twb-bestand niet uit het .twbx-archief extraheren. Het archief is mogelijk leeg of beschadigd.")
                            # Clear session state to prevent showing old data
                            if 'analyse_data' in st.session_state:
                                del st.session_state['analyse_data']
                            return # Stop verdere verwerking

                    # Analyseer het .twb bestand (of het geëxtraheerde .twb bestand)
                    analyse_data = analyseer_tableau_bestand(analysis_path) 
                    
                    # analyseer_tableau_bestand raised exceptions bij fouten, dus als we hier komen is het succesvol
                    st.session_state['analyse_data'] = analyse_data
                    st.session_state['bestandsnaam'] = uploaded_file.name # Originele bestandsnaam
                    st.success("Analyse voltooid!")

                except FileNotFoundError:
                    st.error(f"Bestand niet gevonden: {temp_file}. Dit zou niet moeten gebeuren na een succesvolle upload.")
                except zipfile.BadZipFile:
                    st.error("Fout bij het verwerken van het .twbx-bestand: Het bestand lijkt corrupt of is geen geldig zip-archief.")
                except (KeyError, IndexError): # Gevangen als .twb niet in .twbx zit
                    st.error("Kon geen geldig .twb-bestand vinden in het geüploade .twbx-archief. Controleer de inhoud van het bestand.")
                except ET.ParseError: # Specifiek voor XML parse fouten
                    st.error("Fout bij het parsen van het Tableau-bestand. Controleer of het een geldig .twb XML-bestand is (of correct is geëxtraheerd uit .twbx).")
                except Exception as e: # Vang alle andere exceptions van de analyzer
                    st.error(f"Er is een onverwachte technische fout opgetreden tijdens de analyse.")
                    # Optioneel: toon meer details in een expander voor technische gebruikers
                    with st.expander("Technische details van de fout (voor ontwikkelaars)"):
                        st.text(f"Fouttype: {type(e).__name__}")
                        st.text(f"Foutmelding: {str(e)}")
                    # Clear session state om te voorkomen dat oude data wordt getoond bij een nieuwe fout
                    if 'analyse_data' in st.session_state:
                        del st.session_state['analyse_data']
                finally:
                    # Tijdelijk origineel geüpload bestand verwijderen
                    if os.path.exists(temp_file):
                        try:
                            os.remove(temp_file)
                        except Exception as e_rem_orig:
                            # Loggen naar console/server log, niet per se naar UI
                            print(f"Waarschuwing: kon tijdelijk geüpload bestand {temp_file} niet verwijderen: {e_rem_orig}")
                    
                    # Tijdelijke extractiemap verwijderen indien aangemaakt
                    if temp_dir_for_twbx_extraction and os.path.exists(temp_dir_for_twbx_extraction):
                        try:
                            # shutil.rmtree is nodig voor mappen
                            import shutil
                            shutil.rmtree(temp_dir_for_twbx_extraction)
                        except Exception as e_rem_extr:
                            print(f"Waarschuwing: kon tijdelijke extractiemap {temp_dir_for_twbx_extraction} niet verwijderen: {e_rem_extr}")
        
        # Toon analyse-resultaten als die er zijn
        if 'analyse_data' in st.session_state and st.session_state['analyse_data']:
            analyse_data = st.session_state['analyse_data']
            
            # Samenvatting sectie
            st.subheader("Samenvatting")
            col1, col2, col3 = st.columns(3)
            
            with col1:
                st.metric("Werkbladen", len(analyse_data.get('werkbladen', [])))
            with col2:
                st.metric("Dashboards", len(analyse_data.get('dashboards', [])))
            with col3:
                st.metric("Databronnen", len(analyse_data.get('databronnen', [])))
            
            # Tabbladen voor verschillende secties
            tab_ds, tab_ws, tab_db = st.tabs(["🔌 Databronnen", "📊 Werkbladen", "📋 Dashboards"])
            
            with tab_ds:
                st.subheader("Databronnen")
                databronnen = analyse_data.get('databronnen', [])
                if databronnen:
                    for bron in databronnen:
                        display_datasource(bron)
                else:
                    st.info("Geen databronnen gevonden")
            
            with tab_ws:
                st.subheader("Werkbladen")
                werkbladen = analyse_data.get('werkbladen', [])
                if werkbladen:
                    for ws in werkbladen:
                        display_worksheet(ws)
                else:
                    st.info("Geen werkbladen gevonden")
            
            with tab_db:
                st.subheader("Dashboards")
                dashboards = analyse_data.get('dashboards', [])
                if dashboards:
                    for db in dashboards:
                        display_dashboard(db)
                else:
                    st.info("Geen dashboards gevonden")
            
            # JSON downloaden
            st.subheader("Volledige gegevens")
            json_data = json.dumps(analyse_data, indent=4, ensure_ascii=False)
            st.download_button(
                label="📥 Download JSON",
                data=json_data,
                file_name=f"{os.path.splitext(st.session_state['bestandsnaam'])[0]}_analyse.json",
                mime="application/json"
            )
            
            # Toon een voorbeeld van de JSON (ingekort)
            with st.expander("🔍 Bekijk een voorbeeld van de JSON"):
                st.json(json.loads(json_data))
    
    # Voettekst
    st.markdown("---")
    st.caption("Tableau Analyzer - Gemaakt met Streamlit")

# De display_datasource, display_worksheet, display_dashboard functies zijn hieronder
# ongewijzigd gebleven, maar voor de duidelijkheid van de diff hier ingekort.
# Zorg ervoor dat ze in de uiteindelijke code aanwezig zijn.

def display_datasource(bron):
    """Toon details van een databron"""
    with st.expander(f"🔌 {bron.get('naam', 'Onbekende bron')}"):
        # Algemene informatie
        col1, col2 = st.columns(2)
        with col1:
            st.metric("Aantal verbindingen", len(bron.get('verbindingen', [])))
        with col2:
            st.metric("Aantal kolommen", len(bron.get('kolommen', [])))
        
        # Toon verbindingen
        if 'verbindingen' in bron and bron['verbindingen']:
            st.subheader("Verbindingen")
            verbindingen = []
            for v in bron['verbindingen']:
                verbindingen.append({
                    'type': v.get('class', 'Onbekend'),
                    'server': v.get('server', 'Onbekend'),
                    'database': v.get('dbname', 'Onbekend')
                })
            st.dataframe(pd.DataFrame(verbindingen))
        
        # Toon kolommen
        if 'kolommen' in bron and bron['kolommen']:
            st.subheader("Kolommen")
            kolommen = []
            for k in bron['kolommen']:
                kolommen.append({
                    'naam': k.get('naam', 'Onbekend'),
                    'type': k.get('datatype', 'Onbekend'),
                    'rol': k.get('role', 'Onbekend')
                })
            st.dataframe(pd.DataFrame(kolommen))

def display_worksheet(ws):
    """Toon details van een werkblad"""
    with st.expander(f"📊 {ws.get('naam', 'Onbekend werkblad')}"):
        # Algemene informatie
        col1, col2 = st.columns(2)
        with col1:
            st.metric("Aantal gebruikte databronnen", len(ws.get('gebruikte_databronnen', [])))
        with col2:
            st.metric("Aantal berekende velden", len(ws.get('berekende_velden', [])))
        
        # Toon gebruikte databronnen
        if 'gebruikte_databronnen' in ws and ws['gebruikte_databronnen']:
            st.subheader("Gebruikte databronnen")
            st.write(", ".join(ws['gebruikte_databronnen']))
        
        # Toon berekende velden
        if 'berekende_velden' in ws and ws['berekende_velden']:
            st.subheader("Berekende velden")
            velden = []
            for v in ws['berekende_velden']:
                velden.append({
                    'naam': v.get('naam', 'Onbekend'),
                    'formule': v.get('formule', 'Geen formule')
                })
            st.dataframe(pd.DataFrame(velden))

def display_dashboard(db):
    """Toon details van een dashboard"""
    with st.expander(f"📋 {db.get('naam', 'Onbekend dashboard')}"):
        # Algemene informatie
        st.metric("Aantal objecten", len(db.get('objecten', [])))
        
        # Toon objecten
        if 'objecten' in db and db['objecten']:
            st.subheader("Objecten")
            objecten = []
            for obj in db['objecten']:
                objecten.append({
                    'type': obj.get('type', 'Onbekend'),
                    'naam': obj.get('naam', 'Naamloos'),
                    'grootte': f"{obj.get('breedte', 0)}x{obj.get('hoogte', 0)}"
                })
            st.dataframe(pd.DataFrame(objecten))

if __name__ == "__main__":
    load_custom_css() # Laad CSS voordat de rest van de app wordt getekend
    main() 