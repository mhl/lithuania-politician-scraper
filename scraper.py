#!/usr/bin/env python

from datetime import date, datetime
import json
import re
import requests

import scraperwiki

# This scraper gets its data from the CouchDB API for manoSeimas.lt
# as suggested in this GitHub ticket:
#
#   https://github.com/everypolitician/everypolitician-data/issues/442

r = requests.get('http://couchdb.manoseimas.lt/mps/_design/mps-by-start-date/_view/MPs%20by%20start%20date')

data = r.json()

results = []

terms = {
    '10': {
        'start_date': date(2008, 11, 17),
        'end_date': date(2012, 11, 16),
    },
    '11': {
        'start_date': date(2012, 11, 17),
        'end_date': None,
    }
}

def get_memberships(value, membership_type):
    return [
        g for g in value['groups']
        if g['type'] == membership_type
    ]

class NoGroupFound(Exception):
    pass

def get_membership_one_expected(value, membership_type):
    results = get_memberships(value, membership_type)
    if not results:
        print "Warning: no group found"
        raise NoGroupFound()
    elif len(results) > 1:
        raise Exception, "Found {count} {mtype} groups in {value}".format(
            count=len(results),
            mtype=membership_type,
            value=json.dumps(value, indent=4, sort_keys=True)
        )
    return results[0]

def get_term(start_date, end_date):
    '''For the moment, only return a term for those in the current parliament'''
    if not end_date:
        return '11'
    if end_date >= terms['11']['start_date']:
        return '11'

def get_email(value):
    email_list = value['email']
    if not email_list:
        return None
    email_with_trailing_data = email_list[0]
    return re.sub('\s.*', '', email_with_trailing_data)

def decompose_constituency(value):
    m = re.search(r'^(.*?)\s+\(Nr\.\s*(\d+)\)\s*(.*)$', value['constituency'])
    if m:
        return m.group(1), int(m.group(2))
    else:
        return value['constituency'], None

for row in data['rows']:
    value = row['value']
    full_name = u'{0} {1}'.format(value['first_name'], value['last_name'])
    parl_membership = get_membership_one_expected(value, 'parliament')
    # Some people seem to be missing a party field:
    try:
        party_membership = get_membership_one_expected(value, 'party')
    except NoGroupFound:
        party_membership = {}
    start_date, end_date = [
        None if s is None else datetime.strptime(s, '%Y-%m-%d').date()
        for s in parl_membership['membership']
    ]
    term = get_term(start_date, end_date)
    if not term:
        continue
    area, area_id = decompose_constituency(value)
    start_date = max(start_date, terms['11']['start_date'])
    start_date_for_json = str(start_date) if start_date else None
    end_date_for_json = str(end_date) if end_date else None
    person_data = (
        {
            'id': value['source']['id'],
            'name': full_name,
            'website': value['home_page'],
            'image': value['photo'],
            'area': area,
            'area_id': area_id,
            'group': party_membership.get('name'),
            'start_date': start_date_for_json,
            'end_date': end_date_for_json,
            'term': term,
            'given_name': value['first_name'],
            'family_name': value['last_name'],
            'email': get_email(value),
        }
    )
    scraperwiki.sqlite.save(unique_keys=['id'], data=person_data)
