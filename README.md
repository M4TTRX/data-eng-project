# data-eng-project
5IF INSA - Foundations of data engineering project: Correlation of deaths with power plants (nuclear and thermic) in metropolitan France.
# Getting started

1. Ensure you have Docker installed and running
2. Get your computer's user id by typing in ` id -u` in your bash terminal. As a window user you will have to run this command in your WSL terminal as it will not work on CMD or powershell.
3. Update the `AIRFLOW_UID` in the .env file to the ID you obtained.
4. Build and run the environment using the `docker-compose up` command (run it in the directory of the project). This step will take a while as a lot of images will be downloaded on your computer.
5. 

# Data Sources:
réacteurs nucléaires: https://www.data.gouv.fr/fr/datasets/centrales-de-production-nucleaire-dedf-sa/
centrales thermiques: https://www.data.gouv.fr/fr/datasets/centrales-de-production-thermique-a-flamme-dedf-sa-fioul-gaz-charbon/
décès: https://www.data.gouv.fr/fr/datasets/fichier-des-personnes-decedees/
