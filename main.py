import aiohttp
import asyncio
import random
import dataclasses
import re
from motor.motor_asyncio import AsyncIOMotorClient as Mongo
from yattag import Doc
from aiohttp import web
from datetime import date
from sys import stdout
from os import getenv
import string
from hashvids import hashvid, find_col_statevid
from urllib.parse import urlencode, quote_plus as uriquote


INPUT_NAMES = (
        'submitted[reason_elections][reason][temp_illness]',
        'submitted[elections][requested_for][primary]',
        'submitted[elections][requested_for][special]',
        'submitted[name_contact][last_name]',
        'submitted[name_contact][first_name]',
        'submitted[name_contact][middle_initial]',
        'submitted[name_contact][name_suffix]',
        'submitted[name_contact][date_of_birth][month]',
        'submitted[name_contact][date_of_birth][day]',
        'submitted[name_contact][date_of_birth][year]',
        'submitted[name_contact][phone_number]',
        'submitted[name_contact][email]',
        'submitted[address_live][county2]',
        'submitted[address_live][county2]',
        'submitted[address_live][county]',
        'submitted[address_live][address]',
        'submitted[address_live][address2]',
        'submitted[address_live][city]',
        'submitted[address_live][state]',
        'submitted[address_live][zip]',
        'submitted[primary_election][primary_delivery]',
        'submitted[primary_election][primary_delivery]',
        'submitted[primary_election][primary_delivery]',
        'submitted[primary_election][primary_authorized]',
        'submitted[primary_election][primary_address]',
        'submitted[primary_election][primary_address2]',
        'submitted[primary_election][primary_city]',
        'submitted[primary_election][primary_state]',
        'submitted[primary_election][primary_zip]',
        'submitted[general_election][general_delivery]',
        'submitted[general_election][general_delivery]',
        'submitted[general_election][general_delivery]',
        'submitted[general_election][general_authorized]',
        'submitted[general_election][general_address]',
        'submitted[general_election][general_address2]',
        'submitted[general_election][general_city]',
        'submitted[general_election][general_state]',
        'submitted[general_election][general_zip]',
        'submitted[confirm][confirmation][1]',
        )


@dataclasses.dataclass
class Contact(object):
    forename: str
    midname: str
    surname: str
    suffix: str
    house: int
    street: str
    apt: str
    city: str
    state: str
    zipcode: int
    phone: str
    email: str
    dob: date
    ctyvid: int

    @staticmethod
    def normalize(s):
        nalpha = re.compile(r'[^a-zA-Z0-9\s]')
        return nalpha.sub('', ' '.join(str(s).strip().split())).title()

    def __post_init__(self):
        for field in dataclasses.fields(self):
            if field.type is str:
                normalize = self.__class__.normalize
                if field.name == 'email':
                    normalize = str.lower
                elif field.name == 'state':
                    normalize = str.upper
                normalized = normalize(getattr(self, field.name))
                setattr(self, field.name, normalized)

    @classmethod
    async def find_by_id(cls, cksum):
        cksum = cksum.lower().strip()
        if re.match(r'[0-9a-f]{8}', cksum) is None:
            return
        cull = {'cksums': {'$in': [cksum]}}
        reap = {'_id': 0, 'address': 1, 'dob': 1, 'emails': 1,
                'name': 1, 'party': 1, 'phones': 1, 'monroe_county_id': 1}
        harvest = await DB.pe2020.find_one(cull, reap)
        if harvest is None:
            return
        name = harvest['name']
        name_fields = ('first', 'last', 'middle', 'title')
        forename, surname, mdlname, suffix = (
            name[field] for field in name_fields)
        address = harvest['address']
        address_fields = ('house', 'street', 'apartment',
                          'city', 'state', 'zip')
        house, street, apartment, city, state, zipcode = (
            address[field] for field in address_fields)

        dob, phones, emails = (
            harvest[field] for field in ('dob', 'phones', 'emails'))
        phone = phones[0] if len(phones) > 0 else ''
        email = emails[0] if len(emails) > 0 else ''
        ctyvid = harvest['monroe_county_id']
        return cls(forename, mdlname, surname, suffix,
                   house, street, apartment, city, state, zipcode,
                   phone, email, dob, ctyvid)

    @classmethod
    def from_cdl_dat(cls, record):
        surname, forename, mdlname, suffix = record[1:5]
        house, street, apt = record[5:8]
        city, state, zipcode = record[11:14]
        phone, email = record[15:17]
        month, day, year = record[27].split('/')
        dob = date(int(year), int(month), int(day))
        return cls(forename, mdlname, surname, suffix,
                   house, street, apt, city, state, zipcode,
                   phone, email, dob)

    def address(self):
        return f'{self.house} {self.street}'

    def form_data(self):
        keys = INPUT_NAMES
        address = self.address()
        return (
            (keys[0], 'temp_illness'),
            (keys[1], 'primary'),
            (keys[3], self.surname),
            (keys[4], self.forename),
            (keys[5], self.midname),
            (keys[6], self.suffix),
            (keys[7], self.dob.month),
            (keys[8], self.dob.day),
            (keys[9], self.dob.year),
            (keys[10], self.phone),
            (keys[11], self.email),
            (keys[12], 'mc'),
            (keys[15], address),
            (keys[16], self.apt),
            (keys[17], self.city),
            (keys[18], self.state),
            (keys[19], self.zipcode),
            (keys[22], 'mail'),
            (keys[24], address),
            (keys[25], self.apt),
            (keys[26], self.city),
            (keys[27], self.state),
            (keys[28], self.zipcode),
            (keys[38], '1'),
            ('form_id', 'webform_client_form_1185'),
        )


NB_TOKEN = ''


async def register(req):
    endpoint = 'https://voterreg.dmv.ny.gov/MotorVoter'
    contact = await Contact.find_by_id(req.match_info['hash'])
    if contact is None:
        raise web.HTTPFound(location=endpoint)
    doc, tag, text = Doc().tagtext()
    doc.asis('<!DOCTYPE html>')
    with tag('head'):
        with tag('title'):
            text('Voter Registration')
        with tag('script', type='text/javascript'):
            text('''
                window.onload = function() {
                    document.getElementById('rtv').submit();
                }
                ''')
        with tag('form', method='post', action=endpoint, id='rtv'):
            keys = ('DOB', 'email', 'sEmail', 'zip', 'terms')
            vals = (contact.dob.strftime('%m/%d/%Y'),
                    contact.email, contact.email, contact.zipcode, 'on')
            for key, val in zip(keys, vals):
                doc.input(type='hidden', value=val, name=key)
    return web.Response(text=doc.getvalue(), content_type='text/html')


async def nationbuilder(token, path, method='GET', payload=None, **kwargs):
    uri = ('https://wiltforcongress.nationbuilder.com/api/v1/'
           f'{path}?access_token={token}')
    q = urlencode(kwargs)
    if q != '':
        uri += f'&{q}'
    headers = {'Accept': 'application/json'}
    if method != 'GET':
        headers['Content-Type'] = 'application/json'
    async with aiohttp.ClientSession() as http:
        while True:
            async with http.request(method, uri, json=payload,
                                    headers=headers) as rsp:
                if rsp.status in (429, 403):
                    await asyncio.sleep(10 + random.random() * 10)
                elif rsp.status not in range(200, 300):
                    http.raise_for_status()
                return await rsp.json()


async def tag_contact_with(contact, *tags):
    ctyvid = f'055{contact.ctyvid:09}'
    rsp = await nationbuilder(NB_TOKEN, '/people/search',
                              county_file_id=ctyvid)
    if not len(rsp['results']) > 0:
        return
    uid = rsp['results'][0]['id']
    if uid is None:
        return
    tags = tuple(set(tags) | {'sam_was_here'})
    path = f'/people/{uid}/taggings'
    payload = {'tagging': {'tag': tags}}
    return await nationbuilder(NB_TOKEN, path, 'PUT', payload)


async def autofill_cksum(req):
    endpoint = 'https://www2.monroecounty.gov/elections-absentee-form'
    contact = await Contact.find_by_id(req.match_info['hash'])
    if contact is None:
        raise web.HTTPFound(location=endpoint)
    asyncio.create_task(tag_contact_with(contact, 'vote4robin_absentee'))
    doc, tag, text = Doc().tagtext()
    doc.asis('<!DOCTYPE html>')
    with tag('head'):
        with tag('title'):
            text(f"{contact.forename}'s Mail-in Ballot Application")
        with tag('script', type='text/javascript'):
            text('''
                 window.onload = function() {
                    document.getElementById('abs-ballot-app').submit()
                 }
                 ''')
        with tag('form', method='post', action=endpoint, id='abs-ballot-app'):
            for key, val in contact.form_data():
                doc.input(type='hidden', value=val, name=key)

    return web.Response(text=doc.getvalue(), content_type='text/html')


async def regstat(req):
    endpoint = 'https://www.monroecounty.gov/etc/voter/'
    contact = await Contact.find_by_id(req.match_info['hash'])
    if contact is None:
        raise web.HTTPFound(location=endpoint)
    doc, tag, text = Doc().tagtext()
    doc.asis('<!DOCTYPE html>')
    with tag('head'):
        with tag('title'):
            text('Absentee Ballot Status')
        with tag('script', type='text/javascript'):
            text('''
                 window.onload = function() {
                    document.getElementById('regstat').submit()
                 }
                 ''')
        with tag('form', method='post', action=endpoint, id='regstat'):
            keys = ('lname', 'dobm', 'dobd', 'doby', 'no', 'sname', 'zip')
            vals = (contact.surname, contact.dob.month, contact.dob.day,
                    contact.dob.year, contact.house, contact.street,
                    contact.zipcode)
            for key, val in zip(keys, vals):
                doc.input(type='hidden', value=val, name=f'v[{key}]')
    return web.Response(text=doc.getvalue(), content_type='text/html')


async def address_closest(origin, *terminals):
    batches = []
    terminals = tuple(terminals)
    terminal_chunks = list(terminals)
    while len(terminal_chunks) > 0:
        batches.append(terminal_chunks[:10])
        terminal_chunks = terminal_chunks[min(len(terminals), 10):]
    batches = ('|'.join(dest.replace(' ', '+') for dest in batch)
               for batch in batches)
    api_host = 'maps.googleapis.com'
    endpoints = (f'https://{api_host}/maps/api/distancematrix/json'
                 f'?origins={origin}&destinations={sites}'
                 f'&units=metric&key={DM_TOKEN}' for sites in batches)

    async with aiohttp.ClientSession(raise_for_status=True) as http:
        tasks = [asyncio.create_task(http.get(endpoint))
                 for endpoint in endpoints]
        rsps = await asyncio.gather(*tasks)
        payloads = await asyncio.gather(*(rsp.json() for rsp in rsps))
        elements = [payload['rows'][0]['elements']
                    for payload in payloads]
        if len(elements) > 1:
            for batch in elements[1:]:
                elements[0] += batch
        elements = elements[0]
        closest = terminals[0]
        closest_distance = int(elements[0]['distance']['value'])
        if len(elements) > 1:
            for i, element in enumerate(elements[1:]):
                distance = int(element['distance']['value'])
                if distance < closest_distance:
                    closest = terminals[i+1]
                    closest_distance = distance
        return closest


async def geocode(house, street, postcode):
    cull = {'geo.type': 'Point',
            'geo.coordinates': {'$exists': 1},
            'address.house': house,
            'address.street': street,
            'address.zip': postcode}
    reapc = await DB.geocache.count_documents(cull)
    if reapc > 0:
        reap = {'geo.coordinates': 1}
        harvest = await DB.geocache.find_one(cull, reap)
        lat, lng = harvest['geo']['coordinates']
    else:
        async with aiohttp.ClientSession(raise_for_status=True) as http:
            address = uriquote(f'{house} {street}, {postcode}')
            async with http.get(
                    f'https://maps.googleapis.com/maps/api/geocode/json'
                    f'?address={address}&key={DM_TOKEN}') as rsp:
                payload = await rsp.json()
                latLng = payload['results'][0]['geometry']['location']
                lat, lng = (float(latLng['lat']), float(latLng['lng']))
                address = {'house': house, 'zip': postcode, 'street': street}
                point = {'type': 'Point', 'coordinates': (lat, lng)}
                sow = {'$set': {'address': address, 'geo': point}}
        await DB.geocache.update_many(cull, sow, upsert=True)
    return (lat, lng)


async def epoll_sites(req):
    href = ('https://www.google.com/maps/d/u/0/embed'
            '?mid=1qspTcjcMcSm2Ao4t0ZMyZhN99MHnqm6i')
    house = req.query.get('house')
    street = req.query.get('street')
    postcode = req.query.get('zip')
    if None not in (house, street, postcode):
        lat, lng = await geocode(house, street, postcode)
        coords = ','.join(map(str, (lat, lng)))
        href = f'{href}&ll={coords}&z=11'
    raise web.HTTPFound(location=href)


async def gotv_passthrough(req):
    contact = await Contact.find_by_id(req.match_info['hash'])
    await asyncio.create_task(tag_contact_with(contact, 'vote4robin_gotv_passthrough'))
    raise web.HTTPFound('https://wiltforcongress.com/vote')


async def epoll(req):
    contact = await Contact.find_by_id(req.match_info['hash'])
    if contact is None:
        raise web.HTTPFound('/earlybird_sites')
    triplet = dict(zip(('house', 'street', 'zip'),
                       (contact.house, contact.street, contact.zipcode)))
    await asyncio.create_task(tag_contact_with(contact, 'vote4robin_earlybird'))
    early_polling_sites = (
        '57 St. Paul St., 2nd Floor, Rochester, NY 14604',
        '700 North St., Rochester, NY 14605',
        '310 Arnett Blvd., Rochester, NY 14619',
        '10 Felix St., Rochester, NY 14608',
        '680 Westfall Rd., Rochester, NY 14620',
        '1039 N. Greece Rd., Rochester, NY 14626',
        '1 Miracle Mile Dr., Rochester, NY 14623',
        '1290 Titus Ave., Rochester, NY 14617',
        '3100 Atlantic Ave., Penfield, NY 14526',
        '6720 Pittsford Palmyra Rd., Fairport, NY 14450',
        '4761 Redman Rd., Brockport, NY 14420',
        '1350 Chiyoda Dr., Webster, NY 14580',
    )
    residence = uriquote(f'{contact.house} {contact.street}, {contact.zipcode}')
    cull = {'residence': triplet, 'site': {'$in': early_polling_sites}}
    reapc = await DB.early_polling.count_documents(cull)
    if reapc < 1:
        lat, lng = await geocode(contact.house,
                                 contact.street,
                                 contact.zipcode)
        q = {'type': 'Point', 'coordinates': (lat, lng)}
        q = {'geo': {'$near': {'$geometry': q}}}
        top_three = []
        sites = DB.early_polling_sites.find(q, {'address': 1})
        async for site in sites:
            top_three.append(site['address'])
            if len(top_three) == 3:
                break
        try:
            closest = await address_closest(residence, *top_three)
        except Exception:
            raise web.HTTPFound(location='/earlybird_sites')
        sow = {'$set': {'residence': triplet, 'site': closest}}
        await DB.early_polling.update_many(cull, sow, upsert=True)
    else:
        reap = {'site': 1}
        harvest = await DB.early_polling.find_one(cull, reap)
        closest = harvest['site']
    closest = uriquote(closest)
    doc, tag, text = Doc().tagtext()
    doc.asis('<!DOCTYPE html>')
    with tag('head'):
        with tag('title'):
            text(f"{contact.forename}'s Early Polling Sites")
        with tag('style'):
            text('''
                 a { text-decoration: none;
                     color: #4287f5;
                     font-size: xx-large;
                     padding: 0.5em;
                     font-family: Roboto, Arial, 'sans-serif'; }
                 a:hover { color: #1bf5ee; }
                 div.flex { display: flex;
                            float: center;
                            margin: auto;
                            width: 80%;
                            height: 100vh;
                            padding: 2em;
                            flex-direction: column; }
                 ''')
    with tag('body'):
        closest_src = (r'https://www.google.com/maps/embed/v1/directions'
                       f'?origin={residence}&destination={closest}&key={DM_TOKEN}')
        center = urlencode({'house': contact.house,
                            'street': contact.street,
                            'zip': contact.zipcode})
        browse_src = f'/earlybird_sites?{center}'
        with tag('div', klass='flex'):
            with tag('a', href=f'https://google.com/maps/place/{closest}'):
                text(r'Closest early polling to '
                     f'{contact.house} {contact.street}')
            with tag('iframe', src=closest_src, height='50%'):
                pass
            with tag('a', href=browse_src):
                text('Browse all early polling sites')
            with tag('iframe', src=browse_src, height='50%'):
                pass

    return web.Response(text=doc.getvalue(), content_type='text/html')


if __name__ == '__main__':
    from sys import stdin, stderr, platform
    from argparse import ArgumentParser
    import signal
    import logging

    parser = ArgumentParser()
    parser.add_argument('--log', required=(platform == 'linux'))
    parser.add_argument('--nb-token', required=False)
    parser.add_argument('--dm-token', required=False)
    parser.add_argument('--debug', action='store_true')
    args = parser.parse_args()
    NB_TOKEN = args.nb_token
    if NB_TOKEN is None:
        NB_TOKEN = getenv('NB_TOKEN').strip()
        if NB_TOKEN == '':
            stderr.write('fatal: no NB_TOKEN specified\n')
            exit()
    DM_TOKEN = args.dm_token
    if DM_TOKEN is None:
        DM_TOKEN = getenv('DM_TOKEN').strip()
        if DM_TOKEN == '':
            stderr.write('fatal: no DM_TOKEN specified\n')
            exit()

    signal.signal(signal.SIGINT, signal.SIG_DFL)

    async def static_favicon(req):
        raise web.HTTPFound(location='https://wiltforcongress.com/favicon.ico')

    async def index(req):
        raise web.HTTPFound(location='https://wiltforcongress.com/')

    logging.basicConfig(level=logging.INFO)

    def run():
        global DB
        DB = Mongo('mongodb://voterreg:onboarding@vote4robin.com'
                   '/nationbuilder_replica_test').nationbuilder_replica_test
        app = web.Application()
        app.add_routes([web.get('/', index),
                        web.get('/favicon.ico', static_favicon),
                        web.get('/earlybird_sites', epoll_sites),
                        web.get('/{hash}', autofill_cksum),
                        web.get('/{hash}/vote', gotv_passthrough),
                        web.get('/{hash}/earlybird', epoll),
                        web.get('/{hash}/apply', autofill_cksum),
                        web.get('/{hash}/status', regstat),
                        # web.get('/{hash}/register', register),
                        ])
        web.run_app(app, port=80)

    if not args.debug and platform == 'linux':
        from daemon import DaemonContext
        with (open(args.log, 'a') if args.log is not None else stderr) as ostrm:
            with DaemonContext(stdout=ostrm, stderr=ostrm):
                run()
    else:
        run()
