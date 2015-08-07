#!/usr/bin/env python

from datetime import date, datetime
import json
import requests

import scraperwiki

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
    start_date = max(start_date, terms['11']['start_date'])
    person_data = (
        {
            'id': value['source']['id'],
            'name': full_name,
            'area': value['constituency'],
            'group': party_membership.get('name'),
            'start_date': start_date,
            'end_date': end_date,
            'term': term,
        }
    )
    scraperwiki.sqlite.save(unique_keys=['id'], data=person_data)
