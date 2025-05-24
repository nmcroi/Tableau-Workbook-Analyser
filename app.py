import streamlit as st
import os
import json
import pandas as pd
import io
from datetime import datetime
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
        with st.expander("Berekende formule"):
            st.code(field['formule'], language='sql')

    if 'alias' in field and field['alias']:
        st.caption(f"Weergavenaam: {field['alias']}")

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
                st.write(f"- {feld}")

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
                try:
                    # Analyseer het bestand
                    analyse_data = analyseer_tableau_bestand(temp_file)
                    
                    if analyse_data:
                        # Bewaar de analysegegevens in de sessie
                        st.session_state['analyse_data'] = analyse_data
                        st.session_state['bestandsnaam'] = uploaded_file.name
                        st.success("Analyse voltooid!")
                    else:
                        st.error("Er is een fout opgetreden tijdens het analyseren van het bestand.")
                    
                except Exception as e:
                    st.error(f"Er is een fout opgetreden: {str(e)}")
                finally:
                    # Tijdelijk bestand verwijderen
                    try:
                        os.remove(temp_file)
                    except:
                        pass
        
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
    main()
