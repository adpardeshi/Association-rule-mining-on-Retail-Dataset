import pandas as pd

itemsets = pd.read_parquet("../data/frequent_itemsets")
rules = pd.read_parquet("../data/association_rules")

print(itemsets.head())
print(rules.head())
