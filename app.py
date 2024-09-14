import streamlit as st
import requests
from bs4 import BeautifulSoup
import pandas as pd
from concurrent.futures import ProcessPoolExecutor, as_completed
import base64

# Informations personnelles
#photo_url = "https://github.com/CodingYayaToure/Premier_projet/blob/main/yaya_cncs-modified%20(1).png?raw=true"
photo_url = "CFD_2024.png"
prenom = "Yaya"
nom = "Toure"
email = "yaya.toure@unchk.edu.sn"
whatsapps_url = "https://wa.me/message/GW7RWRW3GR4WN1"
linkedin_url = "https://www.linkedin.com/in/yaya-toure-8251a4280/"
github_url = "https://github.com/CodingYayaToure"
universite = "UNCHK"
formation = "Licence Analyse Numerique et Modelisation | Master Calcul scientifique et Modelisation"
certification = "Collecte et Fouille de Donnees (UADB-CNAM Paris)"

# URL de base pour le scraping avec pagination
base_urls = {
    "Voitures": "https://dakar-auto.com/senegal/voitures-4?page={}",
    "Location voitures": "https://dakar-auto.com/senegal/location-de-voitures-19?page={}",
    "Motos": "https://dakar-auto.com/senegal/motos-and-scooters-3?page={}"
}

def scrape_page(page_number, base_url, category):
    url = base_url.format(page_number)
    response = requests.get(url)
    data = []

    if response.status_code == 200:
        soup = BeautifulSoup(response.text, 'html.parser')
        listings = soup.find_all('div', class_='listing-card')

        for listing in listings:
            try:
                title_element = listing.find('h2', class_='listing-card__header__title')
                marque_modele = title_element.text.strip() if title_element else 'N/A'
                marque, modele = marque_modele.split()[:2] if len(marque_modele.split()) >= 2 else (marque_modele, 'N/A')
                année = marque_modele.split()[-1] if marque_modele.split()[-1].isdigit() else 'N/A'
                attributes = listing.find('ul', class_='listing-card__attribute-list')
                kilometrage, boite_de_vitesse, carburant, référence = 'N/A', 'N/A', 'N/A', 'N/A'
                if attributes:
                    for li in attributes.find_all('li', class_='listing-card__attribute'):
                        if 'km' in li.text:
                            kilometrage = li.text.strip()
                        elif category != "Motos":
                            if 'Automatique' in li.text or 'Manuelle' in li.text:
                                boite_de_vitesse = li.text.strip()
                            elif 'Essence' in li.text or 'Diesel' in li.text:
                                carburant = li.text.strip()
                        if 'Ref.' in li.text:
                            référence = li.text.strip().replace('Ref. ', '')
                price_element = listing.find('h3', class_='listing-card__header__price')
                prix = price_element.text.strip() if price_element else 'N/A'
                town_element = listing.find('span', class_='town-suburb')
                province_element = listing.find('span', class_='province')
                town = town_element.text.strip() if town_element else ''
                province = province_element.text.strip() if province_element else ''
                adresse_de_vente = f"{town}_{province}".replace(' ', '_').replace(',', '_').strip('_')
                owner_element = listing.find('p', class_='time-author')
                proprietaire = owner_element.text.strip().replace(' ', '_') if owner_element else 'N/A'

                if category == "Motos":
                    data.append([marque, modele, année, kilometrage, prix, référence, adresse_de_vente, proprietaire])
                else:
                    data.append([marque, modele, année, kilometrage, boite_de_vitesse, carburant, prix, référence, adresse_de_vente, proprietaire])
            except AttributeError as e:
                print(f"Error processing listing on page {page_number}: {e}")
                continue
    return data

def scrape_data(max_pages, category):
    base_url = base_urls[category]
    data = []
    with ProcessPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(scrape_page, page, base_url, category): page for page in range(1, max_pages + 1)}
        for future in as_completed(futures):
            page_number = futures[future]
            try:
                result = future.result()
                if result:
                    data.extend(result)
                print(f"Page {page_number} scraped successfully.")
            except Exception as e:
                print(f"Error scraping page {page_number}: {e}")

    if category == "Motos":
        columns = ['Marque', 'Modèle', 'Année', 'Kilométrage', 'Prix', 'Référence', 'Adresse_de_Vente', 'Propriétaire']
    else:
        columns = ['Marque', 'Modèle', 'Année', 'Kilométrage', 'Boîte_de_Vitesse', 'Carburant', 'Prix', 'Référence', 'Adresse_de_Vente', 'Propriétaire']
    df = pd.DataFrame(data, columns=columns)
    return df

# Interface Streamlit
st.sidebar.title("Informations personnelles")
st.sidebar.image(photo_url, caption=f"{prenom} {nom}", width=430)
st.sidebar.write(f"Nom: {prenom} {nom}")
st.sidebar.write(f"Email: {email}")
st.sidebar.markdown(f"[Whatsapps]({whatsapps_url})")
st.sidebar.markdown(f"[LinkedIn]({linkedin_url})")
st.sidebar.markdown(f"[GitHub]({github_url})")
st.sidebar.write(f"Université: {universite}")
st.sidebar.write(f"**Formations:** {formation}")
st.sidebar.write(f"**Certification:** {certification}")

st.title("Scraping des données de véhicules")


st.write("""
Bienvenue sur Fluide WEBScrapping, une application professionnelle dédiée à l'extraction automatisée de données de véhicules de https://dakar-auto.com. Que vous soyez à la recherche de voitures, de voitures en location ou de motos, notre outil vous permet de scraper les informations essentielles de manière efficace et en un temps record grâce à la parallélisation. Voici les détails que nous collectons : marque, modèle, année, kilométrage, boîte de vitesse, carburant, prix, référence, adresse de vente, et propriétaire.

### Avantages de la parallélisation dans le web scraping

La parallélisation optimise le web scraping en permettant l'exécution simultanée de multiples tâches de scraping, ce qui réduit considérablement le temps de traitement global. Elle exploite les capacités multicœurs des processeurs, maximisant l'efficacité des ressources matérielles. Cette technique améliore la réactivité de l'application, la rendant idéale pour les mises à jour fréquentes de données. De plus, la parallélisation permet de gérer de grands volumes de données de manière scalable, assurant une collecte fiable et robuste même en cas de défaillance partielle.

En somme, la parallélisation rend Fluide WEBScrapping plus rapide, efficace et fiable, offrant ainsi une expérience optimale pour l'extraction de données de véhicules.
""")

category = st.selectbox("Choisissez la catégorie", ["Voitures", "Location voitures", "Motos"])
max_pages = st.number_input("Nombre de pages à scraper", min_value=1, max_value=2049, value=1)
scrape_button = st.button("Scraper les données")

if scrape_button:
    with st.spinner("Scraping en cours..."):
        df = scrape_data(max_pages, category)
        st.success("Scraping terminé!")
        st.dataframe(df)

    csv = df.to_csv(index=False, encoding='utf-8')
    b64 = base64.b64encode(csv.encode()).decode()  # some strings
    href = f'<a href="data:file/csv;base64,{b64}" download="{category}_data.csv">Télécharger le fichier CSV</a>'
    st.markdown(href, unsafe_allow_html=True)
