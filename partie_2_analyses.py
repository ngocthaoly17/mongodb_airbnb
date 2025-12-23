# Le nombre d’annonces par type de location
db.house_rental.aggregate([
  { $group: { _id: "$property_type", count: { $sum: 1 } } },
  { $sort: { count: -1 } }
])

# Les logements les plus loués
db.house_rental.find(
  {},
  { name: 1, number_of_reviews: 1 }
)
.sort({ number_of_reviews: -1 })
.limit(5)

# Le nombre total d’hôtes différents
db.house_rental.distinct("host_id").length

# Le nombre de locations réservables instantanément
db.house_rental.countDocuments({ instant_bookable: true });

# Les hôtes avec plus de 100 annonces sur les plateformes

db.house_rental.aggregate([
  { $group: { _id: "$host_id", nb_annonces: { $max: "$calculated_host_listings_count" } } },
  { $match: { nb_annonces: { $gt: 100 } } }
])

# Le nombre de super hôtes différents
db.house_rental.distinct("host_id", { host_is_superhost: true }).length

