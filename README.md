# headscalebacktosqlite
I've made and tested this script to help me go from a v0.23.0 PSQL instance back to SQLITE

Obviously take backups before you do this.

I would strongly suggest testing the resulting sqlite database without your production nodes phoning home to it. Test making a user, register a device that isn't part of your production tailnet with a route and enable the route. If all that works you shoud be good to go.

Loose instructions:
1. Make a blank v0.23.0 sqlite.db by running headscale without a database file and the default ```config.yaml```
2. Make a new python env from the directory you want to work in ```python -m venv .```
3. Install dependencies: ```pip install psycopg2-binary pandas SQLAlchemy```
4. Update the ```convert.py``` with the credentials of your psql db just as your headscale config has been accessing the server
5. Run ```python convert.py```
