import pandas as pd
import requests
from datetime import datetime
from sqlalchemy import create_engine,text
from sqlalchemy.engine import URL
import os


#--------------------------------------------------
# Data extraction fucntion 
#--------------------------------------------------
def extract(cities: list, api_key: str) :
  weather_data = []
 
  for city in cities:
    try:
      geo_url = f'http://api.openweathermap.org/geo/1.0/direct?q={city}&appid={api_key}'     
      geo_response = requests.get(geo_url, timeout=30) 
      geo_response.raise_for_status()
      geo_data = geo_response.json() 

      if not geo_data:
        print(f"{city} intouvable")
        continue
      
  #From the geo_data dictionnary we get the latittude and the longitude for each cities   
      lat = geo_data[0]['lat']
      lon = geo_data[0]['lon']

 #Using the latittude and the longitude coordonnates we send a request through the weather_api url 
      weather_url = f'https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={api_key}&lang=fr&units=metric'
      weather_response = requests.get(weather_url, timeout=30)
      weather_response.raise_for_status()
      weather_info = weather_response.json()

 #We get the data we need from the api response files
      city_data = { "city": city,
                  "region": geo_data[0].get("state"),
                  "Temp": weather_info["main"]["temp"],
                  "Temp min": weather_info["main"]["temp_min"],
                  "Temp max": weather_info["main"]["temp_max"],
                  "Humidity": weather_info["main"]["humidity"],
                  "weather_conditon": weather_info["weather"][0]["description"],
                  "wind_speed": weather_info["wind"]["speed"],
                  "Date_time": datetime.fromtimestamp(weather_info["dt"])
                  }

      weather_data.append(city_data) # finally we add the data in weather_data

    except requests.exceptions.RequestException as e:
            # Erreur réseau, timeout, code HTTP d'erreur (401, 404, 500...)
            print(f"[extract] Erreur réseau/API pour {city} : {e}")
            continue
    except (KeyError, IndexError) as e:
            # Réponse JSON incomplète ou de forme inattendue
            print(f"[extract] Données inattendues pour {city} : {e}")
            continue

  df = pd.DataFrame(weather_data)
  return df


#--------------------------------------------------------
# Data transformation function
#-----------------------------------------------------------
def transform(extracted_data) :
  if extracted_data.empty:
        print("[transform] DataFrame vide, rien à transformer.")
        return extracted_data
        
  
  extracted_data.rename(columns = { "Temp":"Temperature (C)",
                              "Temp min":"Temp_min_observed",
                              "Temp max":"Temp_max_observed",
                              "wind_speed":"wind_speed (m/s)"},
                               inplace = True )

  transformed_data = extracted_data.sort_values(by = "Temperature (C)" , ascending = False)

  return transformed_data


#-----------------------------------------------------------
# Data quality check function
#-------------------------------------------------------------
def quality_check(transformed_data, expected_cities) :
  """Contrôle qualité sur les données transformées, avant chargement.

  Lève une ValueError (ce qui fait échouer la tâche Airflow, donc visible
  et alertable dans l'UI / les logs) si un problème CRITIQUE est détecté :
  DataFrame vide, colonne essentielle manquante, ou trop peu de villes
  récupérées. Les problèmes mineurs (quelques valeurs manquantes, valeurs
  limites) sont seulement affichés en avertissement, sans bloquer le
  chargement.
  """
  if transformed_data.empty:
    raise ValueError("[quality_check] DataFrame vide : aucune donnée à charger.")

  issues = []

  # 1. Colonnes essentielles présentes et non nulles
  critical_columns = ["city", "Temperature (C)", "Date_time"]
  for col in critical_columns:
    if col not in transformed_data.columns:
      raise ValueError(f"[quality_check] Colonne manquante : {col}")
    n_missing = transformed_data[col].isna().sum()
    if n_missing > 0:
      issues.append(f"{n_missing} valeur(s) manquante(s) dans '{col}'")

  # 3. Plages de valeurs plausibles
  out_of_range_temp = transformed_data[
      (transformed_data["Temperature (C)"] < -10) | (transformed_data["Temperature (C)"] > 55)
  ]
  if not out_of_range_temp.empty:
    issues.append(f"{len(out_of_range_temp)} température(s) hors plage plausible (-10°C / 55°C)")

  out_of_range_hum = transformed_data[
        (transformed_data["Humidity"] < 0) | (transformed_data["Humidity"] > 100)
    ]
  if not out_of_range_hum.empty:
      issues.append(f"{len(out_of_range_hum)} humidité(s) hors plage (0-100%)")

  # 4. Taux de complétude par rapport aux villes attendues
  n_received = transformed_data["city"].nunique()
  n_expected = len(expected_cities)
  completeness = (n_received / n_expected) if n_expected else 0

  if completeness < 0.5:
    # Moins de la moitié des villes récupérées : on bloque le chargement
    raise ValueError(
        f"[quality_check] Seulement {n_received}/{n_expected} villes récupérées "
    )
  elif completeness < 1.0:
    issues.append(f"seulement {n_received}/{n_expected} villes récupérées ({completeness:.0%})")

  if issues:
    print("[quality_check] Avertissement(s) non bloquant(s) :")
    for issue in issues:
      print(f"  - {issue}")
  else:
    print(f"[quality_check] OK — {n_received} ville(s) validée(s), aucune anomalie détectée.")


#-----------------------------------------------------------
# Data loading function
#-------------------------------------------------------------
def load(transformed_data, target_table, Dag_run) :
  if transformed_data.empty:
        print("[load] DataFrame vide, aucun chargement effectué.")
        return
   
  url = URL.create(
    drivername="postgresql+psycopg2",
    username=os.environ["Weather_DB_USER"],
    password=os.environ["Weather_DB_PASSWORD"],
    host=os.environ["Weather_DB_HOST"],
    port=os.environ["Weather_DB_PORT"],
    database=os.environ["Weather_DB_NAME"]
    )

  engine = create_engine(url)

  insert_query = text(f"""
        INSERT INTO {target_table} (
            city,
            region,
            "Temperature (C)",
            "Temp_min_observed",
            "Temp_max_observed",
            "Humidity",
            weather_conditon,
            "wind_speed (m/s)",
            "Date_time",
            Dag_run
        )
        VALUES (
            :city,
            :region,
            :temperature,
            :temp_min,
            :temp_max,
            :humidity,
            :weather_condition,
            :wind_speed,
            :date_time,
            :Dag_run
        )
        ON CONFLICT (city, Dag_run)
        DO NOTHING;
    """)

  with engine.begin() as connection:

        for _, row in transformed_data.iterrows():

            connection.execute(
                insert_query,
                {
                    "city": row["city"],
                    "region": row["region"],
                    "temperature": row["Temperature (C)"],
                    "temp_min": row["Temp_min_observed"],
                    "temp_max": row["Temp_max_observed"],
                    "humidity": row["Humidity"],
                    "weather_condition": row["weather_conditon"],
                    "wind_speed": row["wind_speed (m/s)"],
                    "date_time": row["Date_time"],
                    "Dag_run": Dag_run
                }
            )

  print(f"[load] {len(transformed_data)} lignes traitées.")