# headscalebacktosqlite
I've made and tested this script to help me go from a v0.23.0 PSQL instance back to SQLITE

Obviously take backups before you do this.

I would strongly suggest testing the resulting sqlite database without your nodes phoning home to it. Test making a user, register a device that isn't part of your production tailnet with a route and enable the route. If all that works you shoud be good to go.
