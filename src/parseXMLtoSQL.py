import os
import sqlite3
import xml.etree.ElementTree as ET

# Load XML from local file in the xml directory
xml_dir = 'xml'
xml_file = 'NIST_SP-800-53_rev5_catalog.xml'
xml_path = os.path.join(xml_dir, xml_file)
tree = ET.parse(xml_path)
root = tree.getroot()

# Define the namespace dictionary
ns = {'oscal': 'http://csrc.nist.gov/ns/oscal/1.0'}

# Create DB
conn = sqlite3.connect('oscal_controls2.db')
cur = conn.cursor()

# Create tables (add other tables as needed)
cur.execute('''
CREATE TABLE IF NOT EXISTS parts (
    part_id TEXT PRIMARY KEY,
    control_id TEXT,
    name TEXT,
    prose TEXT,
    "order" INTEGER,
    FOREIGN KEY(control_id) REFERENCES controls(control_id)
)''')

cur.execute('''
CREATE TABLE IF NOT EXISTS props (
    prop_id TEXT PRIMARY KEY,
    control_id TEXT,
    name TEXT,
    value TEXT,
    ns TEXT,
    FOREIGN KEY(control_id) REFERENCES controls(control_id)
)''')

cur.execute('''
CREATE TABLE IF NOT EXISTS links (
    link_id TEXT PRIMARY KEY,
    control_id TEXT,
    href TEXT,
    rel TEXT,
    media_type TEXT,
    FOREIGN KEY(control_id) REFERENCES controls(control_id)
)''')

cur.execute('''
CREATE TABLE IF NOT EXISTS control_relations (
    parent_control_id TEXT,
    child_control_id TEXT,
    PRIMARY KEY (parent_control_id, child_control_id),
    FOREIGN KEY(parent_control_id) REFERENCES controls(control_id),
    FOREIGN KEY(child_control_id) REFERENCES controls(control_id)
)''')

cur.execute('''
CREATE TABLE IF NOT EXISTS resources (
    uuid TEXT PRIMARY KEY,
    title TEXT,
    location TEXT,
    citation TEXT
)
''')

#--------- handle nested parts,props, and links ---

import uuid

def gen_id():
    return str(uuid.uuid4())

def get_full_text(elem):
    """Recursively get all text, including text in child tags and their tails."""
    if elem is None:
        return ''
    text = elem.text or ''
    for child in elem:
        text += get_full_text(child)
        if child.tail:
            text += child.tail
    return text

for control in root.findall('.//oscal:control', ns):
    cid = control.get('id')

    # PARTS
    for part in control.findall('.//oscal:part', ns):
        part_id = gen_id()
        name = part.get('name')
        prose = part.findtext('prose')
        order = part.attrib.get('order')
        cur.execute('INSERT OR IGNORE INTO parts VALUES (?, ?, ?, ?, ?)', 
            (part_id, cid, name, prose, order))
        print(f'Inserted part {part_id} for control {cid} with name "{name}" and prose "{prose}"')

    # PROPS
    for prop in control.findall('.//oscal:prop',ns):
        prop_id = gen_id()
        name = prop.get('name')
        value = prop.get('value')
        propns = prop.get('ns')
        cur.execute('INSERT OR IGNORE INTO props VALUES (?, ?, ?, ?, ?)', 
            (prop_id, cid, name, value, propns))

    # LINKS
    for link in control.findall('.//oscal:link', ns):
        link_id = gen_id()
        href = link.get('href')
        rel = link.get('rel')
        media_type = link.get('media-type')
        cur.execute('INSERT OR IGNORE INTO links VALUES (?, ?, ?, ?, ?)', 
            (link_id, cid, href, rel, media_type))

    # RELATIONS (children/parent nesting)
    for child in control.findall('.//oscal:control', ns):
        child_id = child.get('id')
        cur.execute('INSERT OR IGNORE INTO control_relations VALUES (?, ?)', 
            (cid, child_id))


# Add this after parsing the XML and before conn.commit()
# Parse and insert resources from <back-matter>
back_matter = root.find('oscal:back-matter', ns)
if back_matter is not None:
    for resource in back_matter.findall('oscal:resource', ns):
        ruuid = resource.get('uuid')
        # Try to get <title> and <rlink> (location) if present
        title_elem = resource.find('oscal:title', ns)
        title = title_elem.text.strip() if title_elem is not None and title_elem.text else None
        location_elem = resource.find('oscal:rlink', ns)
        location = location_elem.get('href') if location_elem is not None else None
        # Get citation text if present
        citation_elem = resource.find('oscal:citation', ns)
        citation = None
        if citation_elem is not None:
            text_elem = citation_elem.find('oscal:text', ns)
            if text_elem is not None:
                citation = get_full_text(text_elem).strip()
            else:
                citation = get_full_text(citation_elem).strip()
        cur.execute('INSERT OR REPLACE INTO resources VALUES (?, ?, ?, ?)', 
            (ruuid, title, location, citation))
        print(f'Inserted resource uuid {ruuid}, title \"{title}\", location \"{location}\", citation \"{citation}\"')


conn.commit()
conn.close()
