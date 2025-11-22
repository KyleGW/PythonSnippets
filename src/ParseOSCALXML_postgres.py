import os
import psycopg2
import psycopg2.extras
import uuid
import xml.etree.ElementTree as ET
from typing import Dict, Optional, List
import json

OSCAL_NS = 'http://csrc.nist.gov/ns/oscal/1.0'
P_TAG = f'{{{OSCAL_NS}}}p'
INSERT_TAG = f'{{{OSCAL_NS}}}insert'
PROP_TAG = f'{{{OSCAL_NS}}}prop'
PART_TAG = f'{{{OSCAL_NS}}}part'
PARAM_TAG = f'{{{OSCAL_NS}}}param'

ns = {'oscal': OSCAL_NS}

def setup_database(cur):
    """Create necessary tables in the database."""
    cur.execute('''
    CREATE TABLE IF NOT EXISTS controls (
        control_id TEXT PRIMARY KEY,
        catalog_id TEXT,
        class TEXT,
        title TEXT,
        label TEXT,
        statement TEXT
    )''')
    cur.execute('''
    CREATE TABLE IF NOT EXISTS parameters (
        parameter_id TEXT PRIMARY KEY,
        control_id TEXT,
        label TEXT,
        guideline TEXT,
        FOREIGN KEY(control_id) REFERENCES controls(control_id)
    )''')
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
    cur.execute('''
    CREATE TABLE IF NOT EXISTS baselines (
        baseline_id SERIAL PRIMARY KEY,
        name TEXT NOT NULL,
        title TEXT,
        last_modified TEXT,
        party_details TEXT,
        version TEXT
    )
    ''')
    cur.execute('''
    CREATE TABLE IF NOT EXISTS baseline_controls (
        baseline_id INTEGER,
        control_id TEXT,
        PRIMARY KEY (baseline_id, control_id),
        FOREIGN KEY(baseline_id) REFERENCES baselines(baseline_id),
        FOREIGN KEY(control_id) REFERENCES controls(control_id)
    )
    ''')
    cur.execute('''
    CREATE TABLE IF NOT EXISTS control_families (
        id SERIAL PRIMARY KEY,
        family_code VARCHAR(8) NOT NULL UNIQUE,
        family_name VARCHAR(255) NOT NULL,
        description TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    ''')

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

def build_control_param_map(root: ET.Element) -> Dict[str, str]:
    """Build a mapping from param_id to its parent control_id."""
    control_param_map = {}
    for control in root.findall(f'.//{{{OSCAL_NS}}}control'):
        control_id = control.get('id')
        for param in control.findall(f'.//{PARAM_TAG}'):
            param_id = param.get('id')
            control_param_map[param_id] = control_id
    return control_param_map

def build_param_labels(
    root: ET.Element,
    control_param_map: Dict[str, str],
    cur,
    debug: bool = False
) -> Dict[str, str]:
    """Build a dictionary of param_id to label and insert parameters into DB."""
    param_labels = {}
    for param in root.findall(f'.//{PARAM_TAG}'):
        param_id = param.get('id')
        parent_control_id = control_param_map.get(param_id)
        label = None
        # 1. Try to get label from <label> element first
        label_elem = param.find(f'{{{OSCAL_NS}}}label')
        if label_elem is not None and label_elem.text:
            label = label_elem.text
        # 2. If not found in <label>, check for <select> with <choice>
        if not label:
            select_elem = param.find(f'{{{OSCAL_NS}}}select')
            if select_elem is not None:
                choices = [
                    choice.text.strip()
                    for choice in select_elem.findall(f'{{{OSCAL_NS}}}choice')
                    if choice.text
                ]
                if choices:
                    label = " | ".join(choices)
        # 3. If not found in <select>, try <prop name="label">
        if not label:
            for prop in param.findall(PROP_TAG):
                if prop.get('name') == 'label':
                    label = prop.get('value')
                    break
        # 4. If not found in <prop>, try the 'label' attribute
        if not label:
            label = param.get('label')
        # 5. If still not found, fallback to param_id
        if param_id:
            param_labels[param_id] = label if label else f"<{param_id}>"
        
        # Get guideline from <guideline> element
        guideline_elem = param.find(f'{{{OSCAL_NS}}}guideline')
        guideline = None
        if guideline_elem is not None:
            paragraphs = [
                p.text.strip()
                for p in guideline_elem.findall(f'{{{OSCAL_NS}}}p')
                if p.text
            ]
            if paragraphs:
                guideline = " ".join(paragraphs)

        # Insert parameter into the database
        cur.execute('''
            INSERT INTO parameters (parameter_id, control_id, label, guideline) 
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (parameter_id) DO NOTHING
        ''', (param_id, parent_control_id, label, guideline))
        if debug:
            print(f' {parent_control_id}: Inserting parameter {param_id} with label {label}, guideline: [ {guideline} ]')
    return param_labels

def get_label(part: ET.Element) -> Optional[str]:
    """Get the label property from a part element."""
    for child in part:
        if child.tag == PROP_TAG and child.attrib.get('name') == 'label':
            return child.attrib.get('value')
    return None

def parse_p(p_elem: ET.Element, param_labels: Dict[str, str]) -> str:
    """Parse a <p> element, replacing <insert> with parameter labels."""
    text = ""
    for node in p_elem.iter():
        if node.tag == P_TAG:
            if node.text:
                text += node.text
        elif node.tag == INSERT_TAG:
            param_id = node.attrib.get('id-ref')
            label = param_labels.get(param_id, f"<{param_id}>")
            text += f"<{label}>"
        if node.tail is not None and '\n' not in node.tail:
            text += node.tail
    return text.strip()

def parse_part(
    part: ET.Element,
    param_labels: Dict[str, str],
    path: Optional[List[str]] = None,
    depth: int = 0
) -> List[Dict]:
    """Recursively parse <part> elements and return a list of fields."""
    if path is None:
        path = []
    fields = []
    label = get_label(part)
    part_id = part.attrib.get('id')
    part_name = part.attrib.get('name')
    current_path = path + [part_id] if part_id else path

    for p_elem in part.findall(P_TAG):
        field = {
            'path': current_path,
            'id': part_id,
            'name': part_name,
            'label': label,
            'text': parse_p(p_elem, param_labels),
            'depth': depth
        }
        fields.append(field)

    for child_part in part.findall(PART_TAG):
        fields.extend(parse_part(child_part, param_labels, current_path, depth + 1))
    return fields

def insert_control(
    cur,
    cid: str,
    group_id: str,
    control_class: str,
    title: str,
    label_value: Optional[str],
    combined_statement: Optional[str]
):
    """Insert a control into the database."""
    cur.execute('''
        INSERT INTO controls (control_id, catalog_id, class, title, label, statement) 
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (control_id) DO NOTHING
    ''', (cid, group_id, control_class, title, label_value, combined_statement))

def parse_groups(
    root: ET.Element,
    cur,
    param_labels: Dict[str, str],
    debug: bool = False
):
    """Parse all groups and their controls, inserting into the database."""
    for group in root.findall('.//oscal:group', ns):
        group_id = group.get('id')
        group_title = group.findtext('oscal:title', default=None, namespaces=ns)
        print(f'Group ID: {group_id}, Title: {group_title}')

        for control in group.findall('.//oscal:control', ns):
            cid = control.get('id')
            control_class = control.attrib.get('class')
            title = control.findtext('oscal:title', default=None, namespaces=ns)
            label_value = None
            combined_statement = None

            for prop in control:
                if prop.get('name') == 'label' and prop.get('class') == 'zero-padded':
                    label_value = prop.get('value')

            # PARTS
            for part in control.findall('.//oscal:part', ns):
                part_id = gen_id()
                name = part.get('name')
                prose = part.findtext('oscal:prose', default=None, namespaces=ns)
                order = part.attrib.get('order')
                cur.execute('''
                    INSERT INTO parts VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (part_id) DO NOTHING
                ''', (part_id, cid, name, prose, order))

            # PROPS
            for prop in control.findall('.//oscal:prop', ns):
                prop_id = gen_id()
                name = prop.get('name')
                value = prop.get('value')
                propns = prop.get('ns')
                cur.execute('''
                    INSERT INTO props VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (prop_id) DO NOTHING
                ''', (prop_id, cid, name, value, propns))

            # LINKS
            for link in control.findall('.//oscal:link', ns):
                link_id = gen_id()
                href = link.get('href')
                rel = link.get('rel')
                media_type = link.get('media-type')
                cur.execute('''
                    INSERT INTO links VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (link_id) DO NOTHING
                ''', (link_id, cid, href, rel, media_type))

            # RELATIONS (children/parent nesting)
            for child in control.findall('.//oscal:control', ns):
                child_id = child.get('id')
                cur.execute('''
                    INSERT INTO control_relations VALUES (%s, %s)
                    ON CONFLICT (parent_control_id, child_control_id) DO NOTHING
                ''', (cid, child_id))

            # Control statement
            for part in control.findall('oscal:part', ns):
                if part.get('name') == 'statement':
                    if debug:
                        print(f'DEBUG: Getting control statement for part: {part.get("name")}, ID: {part.get("id")}')
                    control_statement = []
                    fields = parse_part(part, param_labels)
                    for field in fields:
                        label = field['label'] if field['label'] else ''
                        text = field['text']
                        indent = "    " * field['depth']
                        line = f"{label} {text}".strip()
                        control_statement.append(f"{indent}{line}")
                    combined_statement = "\n".join(control_statement)

            print(f'Group {group_id} - Inserting control {cid} with class {control_class}, title: {title}, label: {label_value}')
            insert_control(cur, cid, group_id, control_class, title, label_value, combined_statement)

def parse_resources(root: ET.Element, cur):
    """Parse and insert resources from <back-matter>."""
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
            cur.execute('''
                INSERT INTO resources VALUES (%s, %s, %s, %s)
                ON CONFLICT (uuid) DO UPDATE SET
                    title = EXCLUDED.title,
                    location = EXCLUDED.location,
                    citation = EXCLUDED.citation
            ''', (ruuid, title, location, citation))
            print(f'Inserted/updated resource uuid {ruuid}, title "{title}", location "{location}", citation "{citation}"')

def populate_control_families(root: ET.Element, cur):
    """
    Populate the control_families table from <group class="family" id="..."><title>...</title></group>
    """
    for group in root.findall('.//oscal:group', ns):
        group_class = group.get('class')
        family_code = group.get('id')
        family_name = group.findtext('oscal:title', default=None, namespaces=ns)
        # Only insert if this is a control family group (not subgroups)
        if group_class == "family" and family_code and family_name:
            # Optionally, you can extract a description from a <part name="overview"> or similar
            description = None
            overview_part = group.find('oscal:part[@name="overview"]', ns)
            if overview_part is not None:
                description = overview_part.text
            cur.execute('''
                INSERT INTO control_families (family_code, family_name, description, updated_at)
                VALUES (%s, %s, %s, NOW()  )
                ON CONFLICT (family_code) DO NOTHING
            ''', (family_code, family_name, description))

def parse_baseline_profile(profile_path: str, cur, baseline_name: str):
    """Parse a profile XML and insert the baseline and its controls, including title, last-modified, party details, and version."""
    tree = ET.parse(profile_path)
    root = tree.getroot()

    # Extract <title>, <last-modified>, <version> from metadata
    title = None
    last_modified = None
    version = None
    party_details = []

    metadata = root.find('oscal:metadata', ns)
    if metadata is not None:
        title_elem = metadata.find('oscal:title', ns)
        if title_elem is not None and title_elem.text:
            title = title_elem.text.strip()
        last_modified_elem = metadata.find('oscal:last-modified', ns)
        if last_modified_elem is not None and last_modified_elem.text:
            last_modified = last_modified_elem.text.strip()
        version_elem = metadata.find('oscal:version', ns)
        if version_elem is not None and version_elem.text:
            version = version_elem.text.strip()
        # Collect party details
        for party in metadata.findall('oscal:party', ns):
            party_info = {}
            party_info['uuid'] = party.get('uuid')
            party_info['type'] = party.get('type')
            name_elem = party.find('oscal:name', ns)
            if name_elem is not None and name_elem.text:
                party_info['name'] = name_elem.text.strip()
            email_elem = party.find('oscal:email-address', ns)
            if email_elem is not None and email_elem.text:
                party_info['email'] = email_elem.text.strip()
            address_elem = party.find('oscal:address', ns)
            if address_elem is not None:
                address_lines = [al.text.strip() for al in address_elem.findall('oscal:addr-line', ns) if al.text]
                city = address_elem.findtext('oscal:city', default='', namespaces=ns)
                state = address_elem.findtext('oscal:state', default='', namespaces=ns)
                postal = address_elem.findtext('oscal:postal-code', default='', namespaces=ns)
                party_info['address'] = ', '.join(address_lines + [city, state, postal])
            party_details.append(party_info)
    # Serialize party_details as a string (JSON-like)
    party_details_str = json.dumps(party_details, ensure_ascii=False)

    # Insert the baseline with extra info and get the ID back
    cur.execute('''
        INSERT INTO baselines (name, title, last_modified, party_details, version) 
        VALUES (%s, %s, %s, %s, %s)
        RETURNING baseline_id
    ''', (baseline_name, title, last_modified, party_details_str, version))
    baseline_id = cur.fetchone()[0]

    # Find all <with-id> elements under <include-controls>
    for with_id in root.findall('.//oscal:include-controls/oscal:with-id', ns):
        control_id = with_id.text.strip()
        cur.execute('''
            INSERT INTO baseline_controls (baseline_id, control_id) 
            VALUES (%s, %s)
            ON CONFLICT (baseline_id, control_id) DO NOTHING
        ''', (baseline_id, control_id))
    print(f'Inserted baseline "{baseline_name}" with title "{title}", last-modified "{last_modified}", version "{version}", and {len(party_details)} party(ies).')

def main():
    debug = False
    
    # PostgreSQL connection parameters
    db_config = {
        'host': 'localhost',
        'database': 'controls_dashboard',
        'user': 'dev',
        'password': 'password',
        'port': 5432
    }
    
    xml_path = os.path.join('xml', 'NIST_SP-800-53_rev5_catalog.xml')
    profile_path = os.path.join('xml', 'NIST_SP-800-53_rev5_MODERATE-baseline_profile.xml')
    
    tree = ET.parse(xml_path)
    root = tree.getroot()

    with psycopg2.connect(**db_config) as conn:
        with conn.cursor() as cur:
            setup_database(cur)
            # Populate control families
            populate_control_families(root, cur)
            #control_param_map = build_control_param_map(root)
            #param_labels = build_param_labels(root, control_param_map, cur, debug)
            #parse_groups(root, cur, param_labels, debug)
            #parse_resources(root, cur)
            # Parse and insert baseline controls from a profile
            #parse_baseline_profile(profile_path, cur, baseline_name="MODERATE")
            conn.commit()

if __name__ == "__main__":
    main()
