#!/usr/bin/python
# -*- coding: utf-8 -*-

import logging
import random
import math
from datetime import datetime
from base64 import b64encode
import threading
import json
import time
import redis

from .utils import get_pokemon_name, get_args
from playhouse.db_url import connect


args = get_args()

redisDb = redis.StrictRedis()

log = logging.getLogger(__name__)
lock = threading.Lock()

class BaseModel():
    db = redis.StrictRedis()

class Pokemon(BaseModel):
    # We are base64 encoding the ids delivered by the api
    # because they are too big for sqlite to handle

    @classmethod
    def get_active(cls):
        keys = cls.db.keys('pogom-pokemons:*')
        pokemons = []
        
        for key in keys:
            content = cls.db.get(key)
            
            if content == "":
                continue
        
            p = json.loads(content)
            p['pokemon_name'] = get_pokemon_name(p['pokemon_id'])
            pokemons.append(p)

        return pokemons

    @classmethod
    def get_stats(cls):
        keys = cls.db.keys('pogom:stats:*')
        pokemons = []
        
        for key in keys:
            temp = key.split(':')
            length = cls.db.hlen(key)
            seen = cls.db.get('pogom:seen:%s' % temp[2])
            
            pokemons.append({
                'pokemon_id': int(temp[2]),
                'count': length,
                'lastseen': datetime.utcfromtimestamp(float(seen)),
            })
        
        pokemons.sort(key=lambda x: x['count'], reverse=True)

        known_pokemon = set(p['pokemon_id'] for p in pokemons)
        unknown_pokemon = set(range(1,151)).difference(known_pokemon)
        pokemons.extend( { 'pokemon_id': i, 'count': 0, 'lastseen': None } for i in unknown_pokemon)
        
        for p in pokemons:
            p['pokemon_name'] = get_pokemon_name(p['pokemon_id'])
        
        return pokemons

    @classmethod
    def get_heat_stats(cls):
        """
        query = (Pokemon
                 .select(Pokemon.pokemon_id, fn.COUNT(Pokemon.pokemon_id).alias('count'), Pokemon.latitude, Pokemon.longitude)
                 .group_by(Pokemon.latitude, Pokemon.longitude, Pokemon.pokemon_id)
                 .order_by(-SQL('count'))
                 .dicts())

        pokemons = list(query)

        known_pokemon = set(p['pokemon_id'] for p in query)
        unknown_pokemon = set(range(1, 151)).difference(known_pokemon)
        pokemons.extend({'pokemon_id': i, 'count': 0} for i in unknown_pokemon)
        for p in pokemons:
            p['pokemon_name'] = get_pokemon_name(p['pokemon_id'])
        """
        
        pokemons = list([])

        return pokemons
    
    @classmethod
    def get_all(cls):
        return cls.db.scan_iter('pogom-pokemons:*')
    
    @classmethod
    def set(cls, key, value):
        expire = value['time_till_hidden']
        log.info("Expire time: %d" % expire)

        cls.db.setex('pogom-pokemons:%s' % key, expire, json.dumps(value))
        
        statskey = 'pogom:stats:%s' % value['pokemon_id']
        seenkey = 'pogom:seen:%s' % value['pokemon_id']
        lastseen = time.time()

        cls.db.hset(statskey, value['encounter_id'], "")
        cls.db.set(seenkey, lastseen)

class Pokestop(BaseModel):    
    @classmethod
    def get_all(cls):
        return cls.db.scan_iter('pogom-pokestops:*')

    @classmethod
    def set(cls, key, value):
        cls.db.set('pogom-pokestops:%s' % key, json.dumps(value))


class Gym(BaseModel):
    UNCONTESTED = 0
    TEAM_MYSTIC = 1
    TEAM_VALOR = 2
    TEAM_INSTINCT = 3
    
    @classmethod
    def get_all(cls):
        return cls.db.scan_iter('pogom-gyms:*')
    
    @classmethod
    def set(cls, key, value):
        cls.db.set('pogom-gyms:%s' % key, json.dumps(value))


def parse_map(map_dict):
    pokemons = {}
    pokestops = {}
    gyms = {}

    cells = map_dict['responses']['GET_MAP_OBJECTS']['map_cells']
    if sum(len(cell.keys()) for cell in cells) == len(cells) * 2:
        log.warning("Received valid response but without any data. Possibly rate-limited?")

    for cell in cells:
        for p in cell.get('wild_pokemons', []):
            if p['encounter_id'] in pokemons:
                continue  # prevent unnecessary parsing

            pokemons[p['encounter_id']] = {
                'encounter_id': b64encode(str(p['encounter_id'])),
                'spawnpoint_id': p['spawn_point_id'],
                'pokemon_id': p['pokemon_data']['pokemon_id'],
                'latitude': p['latitude'],
                'longitude': p['longitude'],
                'time_till_hidden': p['time_till_hidden_ms'] / 1000,
                'disappear_time': p['last_modified_timestamp_ms'] + p['time_till_hidden_ms']
            }

            if p['time_till_hidden_ms'] < 0 or p['time_till_hidden_ms'] > 900000:
                pokemons[p['encounter_id']]['disappear_time'] = p['last_modified_timestamp_ms'] / 1000 + (15 * 60)

        for p in cell.get('catchable_pokemons', []):
            if p['encounter_id'] in pokemons:
                continue  # prevent unnecessary parsing

            log.critical("found catchable pokemon not in wild: {}".format(p))

            pokemons[p['encounter_id']] = {
                'encounter_id': b64encode(str(p['encounter_id'])),
                'spawnpoint_id': p['spawn_point_id'],
                'pokemon_id': p['pokemon_data']['pokemon_id'],
                'latitude': p['latitude'],
                'longitude': p['longitude'],
                'time_till_hidden': p['time_till_hidden_ms'] / 1000,
                'disappear_time':
                    (p['last_modified_timestamp_ms'] + p['time_till_hidden_ms'])
            }

        for f in cell.get('forts', []):
            if f['id'] in gyms or f['id'] in pokestops:
                continue  # prevent unnecessary parsing

            if f.get('type') == 1:  # Pokestops
                if 'lure_info' in f:
                    lure_expiration = f['lure_info']['lure_expires_timestamp_ms']
                    active_pokemon_id = f['lure_info']['active_pokemon_id']
                else:
                    lure_expiration, active_pokemon_id = None, None

                pokestops[f['id']] = {
                    'pokestop_id': f['id'],
                    'enabled': f['enabled'],
                    'latitude': f['latitude'],
                    'longitude': f['longitude'],
                    'last_modified': f['last_modified_timestamp_ms'],
                    'lure_expiration': lure_expiration,
                    'active_pokemon_id': active_pokemon_id
                }

            else:  # Currently, there are only stops and gyms
                gyms[f['id']] = {
                    'gym_id': f['id'],
                    'team_id': f.get('owned_by_team', 0),
                    'guard_pokemon_id': f.get('guard_pokemon_id', None),
                    'gym_points': f.get('gym_points', 0),
                    'enabled': f['enabled'],
                    'latitude': f['latitude'],
                    'longitude': f['longitude'],
                    'last_modified': f['last_modified_timestamp_ms']
                }
    
    log.info("Pokemon: %d, Pokestops: %d, Gyms: %d" % (len(pokemons), len(pokestops), len(gyms)))
    
    for pokemon in pokemons:
        Pokemon.set(pokemon, pokemons[pokemon])
    
    for pokestop in pokestops:
        Pokestop.set(pokestop, pokestops[pokestop])
    
    for gym in gyms:
        Gym.set(gym, gyms[gym])

def create_tables():
    pass
