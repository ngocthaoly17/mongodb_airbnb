from pymongo import MongoClient
import polars as pl

client = MongoClient("mongodb://localhost:27017")
db = client["NosCitesProject"]     # nom de la base
collection = db["house_rental"]   # nom de la collection
data = list(collection.find({}))

cleaned = []
for d in data:
    new_row = {}
    for key, value in d.items():

        # Convert ObjectId → string
        if key == "_id":
            new_row[key] = str(value)
            continue

        # Convert lists → comma-separated string
        if isinstance(value, list):
            # ex: ["Wifi", "TV"] → "Wifi, TV"
            new_row[key] = ", ".join(map(str, value))
            continue

        # Convert dicts → JSON string (optionnel)
        if isinstance(value, dict):
            import json
            new_row[key] = json.dumps(value)
            continue

        # Convert datetime → ISO string
        if hasattr(value, "isoformat"):
            new_row[key] = value.isoformat()
            continue

        new_row[key] = value

    cleaned.append(new_row)


df = pl.DataFrame(cleaned)

print(df.head())
print(df.shape)

# Calculer le taux de réservation moyen par mois par type de logement
df = df.with_columns([
    (1 - (pl.col("availability_30") / 30)).alias("taux_resa_mensuel")
])
result1 = (
    df.group_by("room_type")
      .agg(pl.col("taux_resa_mensuel").mean().alias("taux_reservation_moyen"))
)
print(result1)

# Calculer la médiane des nombre d’avis pour tous les logements
median_reviews = df["number_of_reviews"].median()
print(median_reviews)


# Calculer la médiane des nombre d’avis par catégorie d’hôte
result2 = (
    df.group_by("host_is_superhost")
      .agg(pl.col("number_of_reviews").median().alias("mediane_reviews"))
)
print(result2)

# Calculer la densité de logements par quartier de Paris
result3 = (
    df.group_by("neighbourhood")
      .agg(pl.count().alias("nb_logements"))
      .sort("nb_logements", descending=True)
)
print(result3)

# Identifier les quartiers avec le plus fort taux de réservation par mois
result4 = (
    df.group_by("neighbourhood")
      .agg(pl.col("taux_resa_mensuel").mean().alias("taux_reservation_moyen"))
      .sort("taux_reservation_moyen", descending=True)
)
print(result4)
