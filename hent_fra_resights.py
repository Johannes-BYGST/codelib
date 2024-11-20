import pandas as pd

""" test_file = "c:/Users/b407939/Documents/Excel_ark/sammenligning_bygninger/ejendomsgrunddatasæt.xlsx"
test_excel = pd.read_excel(test_file)
test_excel = test_excel.iloc[0:49, :]
 """

def get_ejendomstyper_for_bfe_numre(dataframe, column_name):
    from resights.resights_tools import EjerfortegnelsenExtractor
    import requests
    import pandas as pd
    import json
    import openpyxl
    from collections import namedtuple
    from resights.resights_setup import ResightsTokenGenerator
    from sqlalchemy import create_engine
    from datetime import datetime

    # resights brugernavn og password
    username = "api@bygst.dk"
    password = "646625HJ"
    token = ResightsTokenGenerator.generate_token(username, password)
    headers = {'Authorization': f"{token}"}  # header to resights request

    # Definer Ejendom tuple
    Ejendom = namedtuple("Ejendom", ["bfe_nummer", "sfe_nummer", "slug", "type", "sub_type"])

    post_bygninger = []

    BFE_numre = dataframe[column_name]
    BFE_numre = BFE_numre.dropna()


    # Iterer over hvert BFE-nummer og foretag en POST request
    for bfe in BFE_numre:
        query = {
            "source": [
                "overview.bfe_number",
                "overview.sfe_number",
                "overview.slug",
                "overview.type",
                "overview.sub_type"
            ],
            "query": [
                {"field": "bfe_number", "value": bfe}
            ]
        }

        post_response = requests.post(
            "https://api.resights.dk/api/v2/properties?", headers=headers, json=query)
        post_bygning_json = json.loads(post_response.content)

        # Gem data fra overview i Ejendom tuples
        for result in post_bygning_json["results"]:
            overview = result.get("overview", {})
            ejendom = Ejendom(
                bfe_nummer=overview.get("bfe_number", None),
                sfe_nummer=overview.get("sfe_number", None),
                slug=overview.get("slug", None),
                type=overview.get("type", None),
                sub_type=overview.get("sub_type", None)
            )
            post_bygninger.append(ejendom)

    # Konverter liste med tuples til pd.DataFrame
    post_bygninger_df = pd.DataFrame(post_bygninger)

    print("Dataframet 'post_bygninger_df' har ", len(post_bygninger_df), "rækker")

    interesting_columns = post_bygninger_df[['bfe_nummer', 'sub_type']]
    
    new_dataframe = pd.merge(dataframe, interesting_columns, left_on=column_name, right_on='bfe_nummer', how="outer")
    new_dataframe = new_dataframe.drop("bfe_nummer", axis=1)

    return post_bygninger_df

""" sammenligning_df = get_ejendomstyper_for_bfe_numre(test_excel, column_name="BFE nr.")

print(sammenligning_df)

# eksporter dataframe fordi det ikke virker som pakke
output_excel_path = 'c:/Users/b407939/Documents/Excel_ark/sammenligning_bygninger/ejendomsgrunddatasæt.xlsx'
sammenligning_df.to_excel(output_excel_path, index=False)
print(f"Data exported to {output_excel_path}") """


def get_ejerlejligheder(dataframe, bfe_column_name, type_column_name):
    from resights.resights_tools import EjerfortegnelsenExtractor
    import requests
    import pandas as pd
    import json
    import openpyxl
    from collections import namedtuple
    from resights.resights_setup import ResightsTokenGenerator

    # resights brugernavn og password
    username = "api@bygst.dk"
    password = "646625HJ"
    token = ResightsTokenGenerator.generate_token(username, password)
    headers = {'Authorization': f"{token}"}  # header to resights request

    
    filtered_df = dataframe[dataframe[type_column_name] == "MOTHER_PROPERTY_CONDOMINIUMS"]

    remaining_df = dataframe[dataframe[type_column_name] != "MOTHER_PROPERTY_CONDOMINIUMS"]


    # Hvad er næste step?
    # Hent BFE numre fra filtered_df

    # Gør kolonnen til en liste af integers
    BFE_numre = filtered_df[bfe_column_name].astype(int).tolist()

    # Med BFE numrene hentet kan jeg nu bruge den gamle kode. Dog skal jeg også have tilføjet to moderejendomme som ikke indgår i ejendomsgrundatasættet fra SAP (borgergade 16 og borgergade... 16? Forstår jeg ikke. Forskellige moderejendomme har samme adresse.)
    BFE_numre.append(6000637)
    BFE_numre.append(100073899)
    print(BFE_numre)

    # Funktion til at hente data med moderejendommens BFE nummer
    def fetch_property_data(sfe_number):
        url = f"https://api.resights.dk/api/v2/properties?sfe_number={sfe_number}"
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            return response.json()
        else:
            print(f"Error fetching data for BFE {sfe_number}: Status code {response.status_code}")
            return None

    # Funktion til at hente data om ejer med ejerlejlighedens BFE nummer
    def fetch_owner_data(bfe_nummer):
        url = f"https://api.resights.dk/api/v2/properties/{bfe_nummer}/owners"
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            return response.json()
        else:
            print(f"Error fetching BBR data for BFE {bfe_nummer}: Status code {response.status_code}")
            return None
        
    # Funktion til at hente data om ejendom information med ejerlejlighedens BFE nummer
    def fetch_lejlighed_data(bfe_nummer):
        url = f"https://api.resights.dk/api/v2/properties/{bfe_nummer}/overview"
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            return response.json()
        else:
            print(f"Error fetching lejlighed data for BFE {bfe_nummer}: Status code {response.status_code}")
            return None


    # Lister til at gemme resultater
    sfe_numre_list = []
    ejerlejlighed_bfe_list = []
    ejerlejlighed_nr_list = []
    ejerlejlighed_ejer_list = []
    ejerlejlighed_vej_list = []
    ejerlejlighed_by_list = []
    ejerlejlighed_postnr_list = []
    ejerlejlighed_land_list = []

    # Iterer over hvert BFE nummer
    for sfe_nummer in BFE_numre:
        properties_data = fetch_property_data(sfe_nummer)
        if properties_data:
            property_results = properties_data.get("results", [])

            if property_results:
                for element in property_results:
                    condos = element.get("condominiums", [])        # Der er (altid) flere ejerlejligheder som indgår under den samme moderejendom.
                    
                    if condos:
                        for condo in condos:
                            condo_bfe = condo.get("bfe_number", None)
                            sfe_nr = condo.get("sfe_number", None)
                            ejerlejlighed_nr = condo.get("condominium_number", None)

                            # Hent ejer fra owner endpointet
                            owner_data = fetch_owner_data(condo_bfe)    #henter ejeren af ejerlejligheden
                            if owner_data:                              # Hvis der er fundet data
                                for ejerlejlighed in owner_data:                    # Loop over dataen
                                    company = ejerlejlighed.get("company")          # Hent variablen "company"
                                    if company:                                                 # Hvis company eksisterer
                                        ejerlejlighed_ejer = company.get("name")                # Hent ejerensnavn

                                    if ejerlejlighed_ejer == "Bygningsstyrelsen":   # Hvis bygningsstyrelsen er fundet som ejer
                                        sfe_numre_list.append(sfe_nr)                       # Tilføj SFE nummer
                                        ejerlejlighed_bfe_list.append(condo_bfe)            # Tilføj ejerlejlighedens BFE nummer
                                        ejerlejlighed_nr_list.append(ejerlejlighed_nr)      # Tilføj ejerlejlighedens nummer
                                        ejerlejlighed_ejer_list.append(ejerlejlighed_ejer)  # Tilføj ejerlejlighedens ejer
                                        ejerlejlighed_vej_list.append(adresse)              # Tilføj ejerlejlighedens adresse
                                        ejerlejlighed_by_list.append(by)                    # Tilføj ejerlejlighedens by
                                        ejerlejlighed_postnr_list.append(post_nr)           # Tilføj ejerlejlighedens post nummer

                                
                                        print(f"SFE {sfe_nr}, EJERLEJLIGHED BFE {condo_bfe}, EJERLEJLIGHED NR. {ejerlejlighed_nr}")

                            # Hent BFE data fra overview endpointet
                            bfe_data = fetch_lejlighed_data(condo_bfe)

                            address = bfe_data.get("address")
                            if address:
                                street_name = address.get("street_name")                # Hent vejnavnet
                                house_number = address.get("house_number")              # Hent husnummeret           
                                adresse = street_name + " " + house_number              # Flet dem sammen
                                by = address.get("zip_name")                            # Hent bynavn
                                post_nr = address.get("zip_code")                       # Hent post nummer

                    else:
                        print(f"Ingen ejerlejligheder fundet for SFE: {sfe_nummer}")        # Denne siger ofte noget i outputtet. Tror det er fordi "condominimuns" går igen flere gange i JSON-filen.

    # Print længderne af de indsamlede lister
    print(len(sfe_numre_list))
    print(len(ejerlejlighed_bfe_list))
    print(len(ejerlejlighed_nr_list))
    print(len(ejerlejlighed_ejer_list))

    # Konverter listerne til en DataFrame
    filtered_df = pd.DataFrame({
        "Ejendomsnavn": ejerlejlighed_vej_list,
        "BFE nr.": ejerlejlighed_bfe_list,
        "Postnr": ejerlejlighed_postnr_list,
        "By": ejerlejlighed_by_list,
        "SFE Number": sfe_numre_list,
        "Ejerlejlighed Nummer": ejerlejlighed_nr_list,
        "Ejerlejlighed Ejer": ejerlejlighed_ejer_list
    })

    new_dataframe = pd.concat([remaining_df, filtered_df])

    return new_dataframe




