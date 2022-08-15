# fluke-laniakea-plugin
a laniakea datasource plugin for reading data from a Fluke DAQ. This plugin allows the user to:
- Specify channel names, numbers and their type (which will appear accordingly in Influx)
- Writing data to influx
- A predetermined polling interval (Influx writes are blocking and may exceed the interval)
- Access the data via the Laniakea Subscribe API
- Granular authenticate access to the plugin

An example configuration file can be found in the main repository `fluke.yaml.example`

# TODO
- [X] Have plugin read config file
- [X] Have plugin read tags from config file
- [X] Create influx bool config parameter for writing to influx db
- [X] Create influx URL and API token config parameters
- [X] Integrate influx writing
- [X] Add SkipTLSVerify config parameter for influx writing
