import requests
import xml.etree.ElementTree as ET

FMI_URL = "https://opendata.fmi.fi/wfs"
FMISID = "100971"  # Example: Helsinki Kaisaniemi
PARAMS = ["t2m", "ws_10min", "r_1h"]

params = {
    "service": "WFS",
    "version": "2.0.0",
    "request": "GetFeature",
    "storedquery_id": "fmi::observations::weather::timevaluepair",
    "fmisid": FMISID,
    "parameters": ",".join(PARAMS),
    "starttime": "2025-11-22T00:00:00Z",
    "endtime": "2025-11-23T00:00:00Z",
}

resp = requests.get(FMI_URL, params=params)
resp.raise_for_status()
root = ET.fromstring(resp.text)
ns = {
    'wml2': 'http://www.opengis.net/waterml/2.0',
    'gml': 'http://www.opengis.net/gml/3.2'
}

for param in PARAMS:
    print(f"\nParameter: {param}")
    # Find MeasurementTimeseries with gml:id ending with -{param}
    for ts in root.findall('.//wml2:MeasurementTimeseries', ns):
        gid = ts.attrib.get('{http://www.opengis.net/gml/3.2}id', ts.attrib.get('gml:id'))
        if gid and gid.endswith(f'-{param}'):
            for pt in ts.findall('wml2:point/wml2:MeasurementTVP', ns):
                t = pt.find('wml2:time', ns)
                v = pt.find('wml2:value', ns)
                print(f"{t.text if t is not None else '?'}: {v.text if v is not None else '?'}")
            break
    else:
        print("No data found.")
