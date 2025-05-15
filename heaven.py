
import requests
import logging

logger = logging.getLogger("ETL")

class Parent:
    def __init__(self, name, distance):
        self.name = name
        self.distance = distance
    def __repr__(self):
        return f"{self.name}: {self.distance} million km"

def get_planetary_data():
    url = "https://api.le-systeme-solaire.net/rest/bodies/"
    response = requests.get(url)
    data = response.json()
    
    planets = []
    for body in data["bodies"]:
        if body.get("isPlanet"):
            distance = body.get("semimajorAxis") / 1000000
            planets.append(Parent(
                name=body["englishName"],
                distance=distance
            ))
    return planets

def sort_ascending(planets):
    for i in range(1, len(planets)):
        current = planets[i]
        j = i - 1
        while j >= 0 and current.distance < planets[j].distance:
            planets[j + 1] = planets[j]
            j -= 1
        planets[j + 1] = current
    return planets

def show_ascending():
    planets = get_planetary_data()
    asc_planets = sort_ascending(planets)
    for planet in asc_planets:
        logger.info(planet)

def show_descending():
    planets = get_planetary_data()
    desc_planets = sort_ascending(planets)[::-1]  
    for planet in desc_planets:
        logger.info(planet)

