from yattag import Doc
from aiohttp import web
from aiohttp.abc import AbstractAccessLogger
from datetime import date
from time import time as unix
from dataclasses import dataclass
from sys import stdout
import string
from hashvids import hashvid, find_col_statevid


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


@dataclass
class Contact(object):
    surname: str
    forename: str
    suffix: str
    middle_initial: str
    street: str
    house: str
    apt: str
    city: str
    state: str
    phone: str
    email: str
    dob: date

    @staticmethod
    def from_record(self, record):
        surname, forename, mi, suffix = record[1:5]
        surname = surname.strip().title()
        forename = forename.strip().title()
        mi = mi.strip()
        if len(mi) > 1:
            mi = mi[0]
        tr = str.maketrans('', '', string.punctuation)
        suffix = suffix.strip().translate(tr).title()
        house = record[5]
        street, apt = map(str.title, record[6:8])
        city, state, postcode = record[11:14]
        city = city.title()
        phone, email = record[15:17]
        email = email.lower()
        month, day, year = record[27].split('/')
        dob = date(int(year), int(month), int(day))
        Contact(surname, forename, mi, suffix, house, street, apt, city, state,
                postcode, phone, email, dob)

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


async def register(req):
    endpoint = 'https://register2vote.org/'
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
                    var msg = 'Your previous Board of Elections filing ' +
                        'has been used to pre-populate an application form ' +
                        'requesting an update to your registration. ' +
                        'Please verify that all information on the ' +
                        'following page is up to date before your ' +
                        'submission.';
                    document.getElementById('rtv').submit();
                }
                ''')
        with tag('form', method='post', action=endpoint, id='rtv'):
            keys = ('lang', 'rvFirstName', 'rvMiddleName', 'rvLastName',
                    'rvResAddr', 'rvResZip', 'rvDob')
            vals = ('en', contact.forename, contact.middle_initial,
                    contact.surname, contact.address(), contact.zip,
                    contact.dob.strftime('%m/%d/%Y'))
            for key, val in zip(keys, vals):
                doc.input(type='hidden', value=val, name=key)
    return web.Response(text=doc.getvalue(), content_type='text/html')


async def autofill_cksum(req):
    endpoint = 'https://www2.monroecounty.gov/elections-absentee-form'
    try:
        contact = CONTACTS[req.match_info['hash']]
    except KeyError:
        raise web.HTTPFound(location=endpoint)
    doc, tag, text = Doc().tagtext()
    doc.asis('<!DOCTYPE html>')
    with tag('head'):
        with tag('title'):
            text(f"{contact.forename}'s Mail-in Ballot Application")
        with tag('script', type='text/javascript'):
            text('''
                 window.onload = function() {
                     var msg = 'Congratulations, visitor! ' +
                         'You are registered to vote, and ' +
                         'Wilt for Congress was able to autofill your ' +
                         'public filing for the sake of convenience ― ' +
                         'please verify that everything in the following ' +
                         'form is up to date before submitting your ' +
                         'mail-in ballot application.';
                     window.alert(msg);
                     document.getElementById('abs-ballot-app').submit();
                 }
                 '''.replace('visitor', contact.forename))
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


class TerseAccessLogger(AbstractAccessLogger):
    def log(self, req, rsp, time):
        timestamp = unix()
        self.logger.info(f'{request.remote} '
                         f'{unix():.3f} '
                         f'{request.path}')


if __name__ == '__main__':
    from sys import stdin, stderr
    from argparse import ArgumentParser
    from daemon import DaemonContext
    import csv
    import signal
    import logging
    parser = ArgumentParser()
    parser.add_argument('--log', required=False)
    args = parser.parse_args()
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
        CONTACTS[key] = Contact.from_record(record)
        RECORDS[key] = record
        keytoid[key] = record[statevid]
    del keytoid
    # print(next(iter(CONTACTS.keys())))

    async def static_favicon(req):
        raise web.HTTPFound(location='https://wiltforcongress.com/favicon.ico')

    app = web.Application()
    app.add_routes([web.get('/favicon.ico', static_favicon),
                    web.get('/{hash}', autofill_cksum),
                    web.get('/{hash}/apply', autofill_cksum),
                    # web.get('/{hash}/register', register),
                    web.get('/{hash}/status', regstat)])
    logging.basicConfig(level=logging.INFO)
    with (open(args.log, 'a') if args.log is not None else stderr) as ostrm:
        with DaemonContext(stdout=ostrm, stderr=ostrm):
            web.run_app(app, port=80)
            # web.run_app(app, port=80, access_log_class=TerseAccessLogger)
