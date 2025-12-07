from __future__ import annotations

from pathlib import Path 
import json 
from typing import List, Dict, Any 

FR_REGIONS = [
    {"region_code": "FR-ARA", "region_name": "Auvergne-Rhône-Alpes"},
    {"region_code": "FR-BFC", "region_name": "Bourgogne-Franche-Comté"},
    {"region_code": "FR-BRE", "region_name": "Bretagne"},
    {"region_code": "FR-CVL", "region_name": "Centre-Val de Loire"},
    {"region_code": "FR-COR", "region_name": "Corse"},
    {"region_code": "FR-GES", "region_name": "Grand Est"},
    {"region_code": "FR-HDF", "region_name": "Hauts-de-France"},
    {"region_code": "FR-IDF", "region_name": "Île-de-France"},
    {"region_code": "FR-NOR", "region_name": "Normandie"},
    {"region_code": "FR-NAQ", "region_name": "Nouvelle-Aquitaine"},
    {"region_code": "FR-OCC", "region_name": "Occitanie"},
    {"region_code": "FR-PDL", "region_name": "Pays de la Loire"},
    {"region_code": "FR-PAC", "region_name": "Provence-Alpes-Côte d'Azur"}
]

ES_REGIONS = [
    {"region_code": "ES-AN", "region_name": "Andalucía"},
    {"region_code": "ES-AR", "region_name": "Aragón"},
    {"region_code": "ES-AS", "region_name": "Principado de Asturias"},
    {"region_code": "ES-IB", "region_name": "Illes Balears"},
    {"region_code": "ES-CN", "region_name": "Canarias"},
    {"region_code": "ES-CB", "region_name": "Cantabria"},
    {"region_code": "ES-CM", "region_name": "Castilla-La Mancha"},
    {"region_code": "ES-CL", "region_name": "Castilla y León"},
    {"region_code": "ES-CT", "region_name": "Catalunya"},
    {"region_code": "ES-VC", "region_name": "Comunitat Valenciana"},
    {"region_code": "ES-EX", "region_name": "Extremadura"},
    {"region_code": "ES-GA", "region_name": "Galicia"},
    {"region_code": "ES-MD", "region_name": "Comunidad de Madrid"},
    {"region_code": "ES-MC", "region_name": "Región de Murcia"},
    {"region_code": "ES-NC", "region_name": "Comunidad Foral de Navarra"},
    {"region_code": "ES-PV", "region_name": "País Vasco"},
    {"region_code": "ES-RI", "region_name": "La Rioja"},
]

DE_REGIONS = [
    {"region_code": "DE-BW", "region_name": "Baden-Württemberg"},
    {"region_code": "DE-BY", "region_name": "Bayern"},
    {"region_code": "DE-BE", "region_name": "Berlin"},
    {"region_code": "DE-BB", "region_name": "Brandenburg"},
    {"region_code": "DE-HB", "region_name": "Bremen"},
    {"region_code": "DE-HH", "region_name": "Hamburg"},
    {"region_code": "DE-HE", "region_name": "Hessen"},
    {"region_code": "DE-MV", "region_name": "Mecklenburg-Vorpommern"},
    {"region_code": "DE-NI", "region_name": "Niedersachsen"},
    {"region_code": "DE-NW", "region_name": "Nordrhein-Westfalen"},
    {"region_code": "DE-RP", "region_name": "Rheinland-Pfalz"},
    {"region_code": "DE-SL", "region_name": "Saarland"},
    {"region_code": "DE-SN", "region_name": "Sachsen"},
    {"region_code": "DE-ST", "region_name": "Sachsen-Anhalt"},
    {"region_code": "DE-SH", "region_name": "Schleswig-Holstein"},
    {"region_code": "DE-TH", "region_name": "Thüringen"},
]

REGION_POINTS: List[Dict[str, Any]] = [

]