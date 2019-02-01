from bs4 import BeautifulSoup
import requests
import pandas as pd
from datetime import datetime

start_time = datetime.now()
url_zoe = "https://www.lacentrale.fr/listing?makesModelsCommercialNames=RENAULT%3AZOE&options="
n_annonces_ppage = 16


# Fonction pour le scrap d'info des annonces
def calc_numpages(nb_annonces):
    if nb_annonces%n_annonces_ppage > 0 :
        num_page = nb_annonces//n_annonces_ppage + 1
    else :
        num_page = nb_annonces//n_annonces_ppage
    return num_page

def get_numpage(region, versions):
    num_page = {}   
    for version in versions :
        url = url_zoe + "&versions=" + version + "&regions=" + region
        site = requests.get(url).text
        soup = BeautifulSoup(site, 'html.parser')
        nb_annonce = int(soup.find("h2",{"class":"titleNbAds bold sizeC"}).text.split(" ")[0])
        num_page[version] = calc_numpages(nb_annonce)
    return num_page

def get_km(add):
    return int(add.find("a").find("div", {"class" : "fieldMileage"}).text.replace("\xa0","").replace("km",""))

def get_price(add):
    return int(add.find("a").find("div", {"class" : "fieldPrice"}).text.replace("\xa0","").replace("€",""))

def get_year(add):
    return int(add.find("a").find("div", {"class" : "fieldYear"}).text.replace("\xa0",""))

def get_vendor_type(add):
    return add.find("a").find("div", {"class" : "typeSellerGaranty"}).text.split(' ')[0]


# Fonction pour la récolte de liens et la création de soups
def get_link_from_div(div_add_list):
    return div_add_list.find('a')['href']

def get_add_link(soup):
    div_add_list = soup.find_all('div',{'class':'adLineContainer'})
    return list(map(get_link_from_div,div_add_list))

def get_tel(soup):
    return soup.find("div",{"class" : "phoneNumberContent"}).find("span", {"class" : "bold"}).text.replace('\xa0', " ").strip()[:14]

def complete_link(link):
    return 'https://www.lacentrale.fr' + link

def convert_link(link):
    return link.text

def to_soup(page):
    return BeautifulSoup(page,'html.parser')

# Fonctions pour le scrap des argus
def get_argus_years (list_year):
    return int(list_year.find("a").text.strip())

def get_argus_links (list_year):
    return list_year.find("a")['href']


# Fonction d'extraction des infos
def get_infos(versions, regions):
    start_time = datetime.now()
    df = pd.DataFrame({'brand':[],'model':[],'version':[],'year':[],'km':[],'price':[],'tel':[],'vendor_type':[]})
    for region in regions :
        start3 = datetime.now()
        num_page = get_numpage(region, versions)
        print(num_page)
        for version in versions :
            pages = range(num_page[version]-1)
            for i, page in enumerate(pages) :
                if i == 0:
                    url = url_zoe + "&versions=" + version + "&regions=" + region
                else:
                    url = url_zoe + "&page" + str(page+1) + "&versions=" + version + "&regions=" + region
                site = requests.get(url).text
                soup = BeautifulSoup(site, 'html.parser')
                
                # Obtention du lien de chaque annonce de la page
                add_links = get_add_link(soup)
                links = list(map(complete_link, add_links))
                pages_annonces = list(map(convert_link, map(requests.get,links)))
                soup_pages = list(map(to_soup, pages_annonces))
                # Obtention des numéros de tel pour chaque annonce
                tel = list(map(get_tel, soup_pages))

                # Obtention des containers de chaque annonce
                adds = soup.find("div",{"class":"resultListContainer"}).find_all("div",{"class":"adContainer"})
                # Extraction des infos km, prix, année, type de vendeur depuis chaque container
                km = list(map(get_km,adds))
                price = list(map(get_price,adds))
                year = list(map(get_year,adds))
                vendor_type = list(map(get_vendor_type,adds))
                df_inter = pd.DataFrame({'brand' : 'RENAULT', 'model' : 'ZOE', 'version': version ,'year' : year,'km' : km,'price' : price,'tel' : tel,'vendor_type' : vendor_type})
                
                # Ajout des derniers éléments dans le df global
                df = pd.concat([df, df_inter])
    print (datetime.now() - start_time)
    df.reset_index()
    return df


# Fonction pour le scrap de l'argus
def get_argus_links(a) :
    return a['href']

def complete_argus_links(link):
    return 'https://www.lacentrale.fr/' + link


url_argus = 'https://www.lacentrale.fr/cote-voitures-renault-zoe---.html'
argus = requests.get(url_argus).text
soup = BeautifulSoup(argus, 'html.parser')
   
list_year = soup.find_all("li",{'class':'floatL mR10 mB10'})
year = list(map(get_argus_years, list_year))


def get_argus(versions, url_argus):
    df_argus = pd.DataFrame({"year" : [], "version" : [], "argus" : []})
    for y in year :
        url_y = url_argus[:-6] + str(y) + url_argus[-6:]
        site_y = requests.get(url_y).text
        soup_y = BeautifulSoup(site_y, 'html.parser')
        a_list = soup_y.find("div",{"class":"listingResult"}).find_all("a")
        argus_year_links = list(map(complete_argus_links,list(map(get_argus_links, a_list))))
        
        version_argus_0 = []
        version_argus_1 = []
        version_argus_2 = []

        for link in argus_year_links :
            if versions[0] in link :
                site = requests.get(link).text
                soup = BeautifulSoup(site,'html.parser')
                argus = int(soup.find("div",{"class":"boxQuotTitle"}).find("span").text.replace(" ","")) 
                version_argus_0.append(argus) 
            elif versions[1] in link :
                site = requests.get(link).text
                soup = BeautifulSoup(site,'html.parser')
                argus = int(soup.find("div",{"class":"boxQuotTitle"}).find("span").text.replace(" ","")) 
                version_argus_1.append(argus) 

            elif versions[2] in link :
                site = requests.get(link).text
                soup = BeautifulSoup(site,'html.parser')
                argus = int(soup.find("div",{"class":"boxQuotTitle"}).find("span").text.replace(" ","")) 
                version_argus_2.append(argus) 
        
        df_v0 = pd.DataFrame({"year" : y, "version" : versions[0], "argus" : version_argus_0})
        df_v1 = pd.DataFrame({"year" : y, "version" : versions[1], "argus" : version_argus_1})
        df_v2 = pd.DataFrame({"year" : y, "version" : versions[2], "argus" : version_argus_2})

        df_argus = pd.concat([df_argus, df_v0, df_v1, df_v2])
    df_argus.reset_index()
    return df_argus

versions = ["intens","life","zen"]
regions = ["FR-NAQ","FR-PAC","FR-IDF"]

df_argus = get_argus(versions, url_argus)
argus_min = df_argus.groupby(['year','version'])['argus'].min().to_frame().reset_index()
argus_max = df_argus.groupby(['year','version'])['argus'].max().to_frame().reset_index()
argus_mean = df_argus.groupby(['year','version'])['argus'].mean().to_frame().reset_index()

# Scrap des argus min, max et moyen pour chaque année et chaque modèle
df_argus_final = pd.merge(pd.merge(argus_min,argus_max, on=['year','version']),argus_mean, on=['year','version']).rename(columns={'argus_x':'argus_min','argus_y':'argus_max','argus':'argus_mean'})

# Scrap de chaque annonce
df_infos = get_infos(versions, regions)

# Merge des deux dataframes
df_final = pd.merge(df_argus_final, df_infos, on=['year','version'])
