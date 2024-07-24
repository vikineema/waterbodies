# Styling
This folder is to store SLD styles for use with QGIS for the waterbodies product.
It contains a useful script for converting files from SLD v1.1 (created in QGIS) to SLD v1.0 (required for GeoServer).

## Set up
`lxml` is required to run the script. Install by running
```
pip install lxml
```

## Getting an SLD file from QGIS
Load the vector dataset you want to style, choose a "Single symbol" or "Rule-based" styling and customize as needed.

Save the style to `waterbodies/styling/sld_v1_1`.

## Script
To convert scripts from SLD v1.1 to SLD v1.0, run the following command

```
python sld11-10.py sld_v1_1 sld_v1_0
```

Where the first command line argument is the folder containing the SLD v1.1 file, and the second command line argument is the destination folder for the SLD v1.0 file.

## Validation and troubleshooting

The script works by mapping the xml schema for v1.1 to v1.0, but there are some things that don't quite work for GeoServer.

### Single symbol

GeoServer is sensitive to fill style. For example, choosing a hatched pattern in QGIS produces the tag `<se:WellKnownName>slash</se:WellKnownName>` in SLD v1.1.

The script converts this to `<WellKnownName>slash</WellKnownName>` in SLD v1.0. 

However the style does not display correctly, so `slash` must be replaced with `shape://times`. 

For examples of different fill styles for SLD v1.0, see the [GeoServer Documentation](https://docs.geoserver.geo-solutions.it/edu/en/pretty_maps/patterns_dash_arrays.html)

### Rule based

When running the conversion script for a Rule-based SLD file, the following error can appear:

```
sld_v1_1/waterbodies_area_rules.sld sld_v1_1/waterbodies_area_rules.sld:114:0:ERROR:SCHEMASV:SCHEMAV_ELEMENT_CONTENT: Element '{http://www.opengis.net/se}Title': This element is not expected. Expected is one of ( {http://www.opengis.net/se}Description, {http://www.opengis.net/se}LegendGraphic, {http://www.opengis.net/ogc}Filter, {http://www.opengis.net/se}ElseFilter, {http://www.opengis.net/se}MinScaleDenominator, {http://www.opengis.net/se}MaxScaleDenominator, {http://www.opengis.net/se}Symbolizer, {http://www.opengis.net/se}LineSymbolizer, {http://www.opengis.net/se}PolygonSymbolizer, {http://www.opengis.net/se}PointSymbolizer )
```

It's worth uploading the file as is and validating it in GeoServer.
If it validates correctly, the error can be ignored.

### General troubleshooting

The [GeoServer SLD Cookbook](https://docs.geoserver.org/latest/en/user/styling/sld/cookbook/polygons.html) is a good reference for some basic SLD files that will work for GeoServer.

If a file doesn't validate correctly, try comparing the SLD file to the most similar example from the Cookbook.