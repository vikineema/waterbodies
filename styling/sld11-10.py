# Adapted from https://stackoverflow.com/questions/45860004/converting-sld-from-1-1-to-1-0-via-python

from lxml import etree
import os
import sys

sld10 = etree.XMLSchema(
    etree.parse("http://schemas.opengis.net/sld/1.0.0/StyledLayerDescriptor.xsd"))
sld11 = etree.XMLSchema(
    etree.parse("http://schemas.opengis.net/sld/1.1.0/StyledLayerDescriptor.xsd"))
transform = etree.XSLT(etree.parse("sld11-10.xsl"))

def walk(sour_dir: str, dest_dir: str) -> None:
    if not os.path.exists(dest_dir):
        os.mkdir(dest_dir)
    for item in os.listdir(sour_dir):
        sour_path = os.path.join(sour_dir, item)
        dest_path = os.path.join(dest_dir, item)
        if os.path.isdir(sour_path):
            walk(sour_path, dest_path)
        else:
            sour_doc = etree.parse(sour_path)
            if not sld11.validate(sour_doc):
                print(sour_path, sld11.error_log.last_error)
            dest_doc = transform(sour_doc)
            if not sld10.validate(dest_doc):
                print(dest_path, sld10.error_log.last_error)
            dest_doc.write(dest_path,
                pretty_print=True, xml_declaration=True, encoding="UTF-8")
    return None

if __name__ == "__main__":
    walk(sys.argv[1], sys.argv[2])