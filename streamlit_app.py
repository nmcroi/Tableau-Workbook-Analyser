from app import load_custom_css, main

# Dedicated Streamlit entrypoint for Streamlit Cloud
# This ensures the platform runs the Streamlit UI (app.py) instead of the CLI (tableau_analyzer.py)

if __name__ == "__main__":
    load_custom_css()
    main()
