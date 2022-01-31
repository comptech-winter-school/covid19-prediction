cd ~/COVID-19 && git pull
cd ~/covid19-prediction && git pull
source covid19-prediction/venv/bin/activate
pip-sync
echo 'ACCESS_KEY_ID=${{ secrets.ACCESS_KEY_ID }}' > covid19-prediction/.env
echo 'SECRET_ACCESS_KEY_ID=${{ secrets.SECRET_ACCESS_KEY_ID }}' >> covid19-prediction/.env
doit
