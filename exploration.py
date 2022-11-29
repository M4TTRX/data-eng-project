import os
import datetime
PLANT_INSERTION_QUERIES_PATH = 'dags/sql/tmp/plant_insert.sql'

df_thermal = pd.read_csv('dags/data/staging/thermal_plants_clean.csv')
# drop duplicate values of in the plant column of df_thermal
df_thermal.drop_duplicates(subset=['plant'], inplace=True)

df_nuclear = pd.read_csv('dags/data/staging/nuclear_clean_datas.csv')
# drop duplicate values of in the plant column of df_nuclear
df_nuclear.drop_duplicates(subset=['plant'], inplace=True)


query = ''
for _, plant in df_thermal.iterrows():
    position = plant['position'].split(',')
    query += f"INSERT INTO power_plants VALUES ('{plant['plant']}', 'THERMAL', '{plant['fuel']}', '{plant['start_date']}', '{plant['power (MW)']}', '{position[0]}', '{position[1]}');\n"
for _, plant in df_nuclear.iterrows():
    position = plant['position'].split(',')
    query += f"INSERT INTO power_plants VALUES ('{plant['plant']}', 'NUCLEAR', '{plant['fuel']}', '{plant['start_date']}', '{plant['power (MW)']}', '{position[0]}', '{position[1]}');\n"

# Save sql querys
if (os.path.exists(PLANT_INSERTION_QUERIES_PATH)):
    os.remove(PLANT_INSERTION_QUERIES_PATH)
with open(PLANT_INSERTION_QUERIES_PATH, "w") as f:
    f.write(query)
print("Created SQL query")


# parse string to datetime
print(str(datetime.datetime.strptime('19990618', "%Y%m%d"))[:10])
