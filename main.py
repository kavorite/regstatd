import aiohttp
import asyncio
from yattag import Doc
from aiohttp import web
from datetime import date
from sys import stdout
from os import getenv
import string
from hashvids import hashvid, find_col_statevid
from urllib.parse import urlencode


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


class Contact(object):
    def __init__(self, record):
        surname, forename, mi, suffix = record[1:5]
        self.surname = surname.strip().title()
        self.forename = forename.strip().title()
        mi = mi.strip()
        if len(mi) > 1:
            mi = mi[0]
        self.middle_initial = mi
        tr = str.maketrans('', '', string.punctuation)
        suffix = suffix.strip().translate(tr).title()
        self.suffix = suffix
        house, street, apt = record[5:8]
        self.street = street.title()
        self.house = house
        self.apt = apt.title()
        city, self.state, self.zip = record[11:14]
        self.city = city.title()
        self.phone, email = record[15:17]
        self.email = email.lower()
        month, day, year = record[27].split('/')
        self.dob = date(int(year), int(month), int(day))
        self.statevid = record[38]

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
            (keys[5], self.middle_initial),
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
            (keys[19], self.zip),
            (keys[22], 'mail'),
            (keys[24], address),
            (keys[25], self.apt),
            (keys[26], self.city),
            (keys[27], self.state),
            (keys[28], self.zip),
            (keys[38], '1'),
            ('form_id', 'webform_client_form_1185'),
        )


CONTACTS: dict = {}
RECORDS: dict = {}
NB_TOKEN = ''


async def register(req):
    endpoint = 'https://voterreg.dmv.ny.gov/MotorVoter'
    try:
        contact = CONTACTS[req.match_info['hash']]
    except KeyError:
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
                    contact.email, contact.email, contact.zip, 'on')
            for key, val in zip(keys, vals):
                doc.input(type='hidden', value=val, name=key)
    return web.Response(text=doc.getvalue(), content_type='text/html')


async def nationbuilder(token, path, method='GET', payload=None, **kwargs):
    uri = ('https://wiltforcongress.nationbuilder.com/api/v1'
           f'{path}?access_token={token}&')
    uri += urlencode(kwargs)
    async with aiohttp.ClientSession(raise_for_status=True) as http:
        req = http.request(method, uri, json=payload,
                           headers={'Accept': 'application/json'})
        return (await req)


async def tag_contact_respondent(contact):
    rsp = await nationbuilder(NB_TOKEN, '/people/search',
                              state_file_id=contact.statevid)
    payload = await rsp.json()
    u = payload['results'][0]
    tags = tuple(set(u['tags']) | {'vote4robin_absentee'})
    uid = u['id']
    updated = {'tags': tags, 'crc32': hashvid(contact.statevid)}
    rsp = nationbuilder(NB_TOKEN, f'/people/{uid}', 'PUT',
                        payload={'person': updated})
    try:
        return (await rsp)
    except aiohttp.ClientResponseError as exn:
        if exn.status == 429:
            await asyncio.sleep(10)
            return await tag_contact_respondent(contact)
        else:
            raise exn


async def autofill_cksum(req):
    endpoint = 'https://www2.monroecounty.gov/elections-absentee-form'
    try:
        contact = CONTACTS[req.match_info['hash']]
    except KeyError:
        raise web.HTTPFound(location=endpoint)
    asyncio.ensure_future(tag_contact_respondent(contact))
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
    try:
        contact = CONTACTS[req.match_info['hash']]
    except KeyError:
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
                    contact.zip)
            for key, val in zip(keys, vals):
                doc.input(type='hidden', value=val, name=f'v[{key}]')
    return web.Response(text=doc.getvalue(), content_type='text/html')


if __name__ == '__main__':
    from sys import stdin, stderr
    from argparse import ArgumentParser
    # from daemon import DaemonContext
    import csv
    import signal
    import logging
    parser = ArgumentParser()
    parser.add_argument('--log', required=False)
    parser.add_argument('--token', required=True)
    args = parser.parse_args()
    NB_TOKEN = args.token
    signal.signal(signal.SIGINT, signal.SIG_DFL)
    stderr.write('hydrate voter registrations...\n')
    keytoid: dict = {}
    istrm = csv.reader(stdin)
    statevid = find_col_statevid(istrm)
    for record in csv.reader(stdin):
        for i in range(len(record)):
            record[i] = record[i].strip()
        key = hashvid(record[statevid])
        if key in CONTACTS and keytoid[key] != record[statevid]:
            other = keytoid[key]
            msg = f'{record[statevid]}: hash collision found with {other}'
            raise ValueError(msg)
        CONTACTS[key] = Contact(record)
        RECORDS[key] = record
        keytoid[key] = record[statevid]
    del keytoid
    # print(next(iter(CONTACTS.keys())))

    async def static_favicon(req):
        raise web.HTTPFound(location='https://wiltforcongress.com/favicon.ico')

    async def index(req):
        raise web.HTTPFound(location='https://wiltforcongress.com/')

    app = web.Application()
    app.add_routes([web.get('/', index),
                    web.get('/favicon.ico', static_favicon),
                    web.get('/{hash}', autofill_cksum),
                    web.get('/{hash}/apply', autofill_cksum),
                    # web.get('/{hash}/register', register),
                    web.get('/{hash}/status', regstat)])
    logging.basicConfig(level=logging.INFO)
    with (open(args.log, 'a') if args.log is not None else stderr) as ostrm:
        # with DaemonContext(stdout=ostrm, stderr=ostrm):
        web.run_app(app, port=80)
