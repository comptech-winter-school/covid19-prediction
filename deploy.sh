cd ~/COVID-19 && git pull
cd ~/covid19-prediction && git pull
source venv/bin/activate
pip-sync
echo 'ACCESS_KEY_ID=${{ secrets.ACCESS_KEY_ID }}' > .env
echo 'SECRET_ACCESS_KEY_ID=${{ secrets.SECRET_ACCESS_KEY_ID }}' >> .env
doit
