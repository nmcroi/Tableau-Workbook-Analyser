# Tableau Workbook Analyzer

## 📌 Doel van het Project

De Tableau Workbook Analyzer is ontwikkeld om inzicht te bieden in de structuur en samenstelling van Tableau werkboeken (.twb en .twbx bestanden). Het stelt gebruikers in staat om snel een overzicht te krijgen van alle componenten binnen een Tableau werkboek, waaronder:

- Gebruikte databronnen en hun verbindingen
- Werkbladen en hun onderliggende elementen
- Dashboards en hun opbouw
- Berekende velden en hun formules
- Gebruikte parameters en filters

## 🛠️ Functionaliteiten

### 1. Bestandsanalyse

- Ondersteuning voor zowel .twb als .twbx bestanden
- Automatische extractie van .twb uit .twbx archieven
- Gedetailleerde metadata-extractie

### 2. Gebruikersinterface

- Intuïtieve webgebaseerde interface gebouwd met Streamlit
- Overzichtelijke weergave van analyse-resultaten
- Mogelijkheid om aanvullende bestanden te uploaden voor context

### 3. Uitvoer

- Gestructureerde JSON-export van alle geëxtraheerde gegevens
- Visuele samenvatting van belangrijkste componenten
- Downloadbare rapporten

## 🚧 Uitdagingen en Beperkingen

### Huidige Uitdagingen

#### Prestaties met grote bestanden

- Grote Tableau bestanden kunnen traag zijn om te analyseren
- Geheugengebruik kan hoog oplopen bij complexe werkboeken

#### Beperkte ondersteuning

- Niet alle Tableau specifieke functies worden volledig ondersteund
- Beperkte ondersteuning voor aangepaste SQL-query's in gegevensbronnen

#### Gebruikerservaring

- De interface kan nog worden verbeterd voor niet-technische gebruikers
- Beperkte visualisatiemogelijkheden binnen de app zelf

## 🚀 Toekomstvisie

### Geplande Verbeteringen

#### Uitgebreide Analyse

- Toevoegen van meer gedetailleerde prestatiemetingen
- Analyse van berekende velden en hun impact
- Detectie van mogelijke optimalisaties

#### Verbeterde Gebruikerservaring

- Meer interactieve visualisaties
- Aangepaste rapportage-opties
- Export naar verschillende formaten (PDF, Excel, etc.)

#### Uitbreiding Functionaliteit

- Ondersteuning voor Tableau Server/Online integratie
- Automatische documentatiegeneratie
- Vergelijkende analyse tussen verschillende versies van hetzelfde werkboek

#### Technische Verbeteringen

- Optimalisatie van de verwerkingssnelheid
- Betere foutafhandeling en gebruikersfeedback
- Uitgebreidere testdekking

## 🛠️ Technische Vereisten

- Python 3.9+
- Vereiste packages zijn te vinden in requirements.txt

## 📦 Installatie

```bash
# Kloon de repository
git clone [repository-url]

# Ga naar de projectmap
cd project-tableau-analyzer

# Maak een virtuele omgeving aan en activeer deze
python -m venv venv
source venv/bin/activate  # Op Windows: venv\Scripts\activate

# Installeer de benodigde packages
pip install -r requirements.txt

# Start de applicatie
streamlit run app.py
```

## 🚀 Snelle Start

```bash
cd "/Users/ncroiset/Vibe Coding Projecten/Cursor Projecten/Project Tableau" && source venv/bin/activate && streamlit run app.py
```

## 🤝 Bijdragen

Bijdragen aan dit project zijn welkom! Voel je vrij om een issue aan te maken of een pull request in te dienen.

## 📄 Licentie

Dit project is gelicentieerd onder de MIT Licentie. 