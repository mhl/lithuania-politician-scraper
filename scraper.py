#!/usr/bin/env python
# -*- coding: utf-8 -*-

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

def get_phone(value):
    first_phone_value = value['phone'][0]
    if not first_phone_value:
        return None
    # Only allow numbers, spaces or brackets in the phone number:
    return re.search(r'^([0-9\(\) ]+)', first_phone_value).group(1)

def clip_dates(start_date, end_date, overall_start_date, overall_end_date):
    assert start_date is not None
    assert overall_start_date is not None
    new_start_date = max(start_date, overall_start_date)
    if end_date and overall_end_date:
        new_end_date = min(end_date, overall_end_date)
    elif end_date:
        new_end_date = end_date
    elif overall_end_date:
        new_end_date = overall_end_date
    else:
        new_end_date = None
    return new_start_date, new_end_date

FRACTION_POSITIONS = [
    u'Frakcijos narys',
    u'Frakcijos narė',
    u'Frakcijos seniūno pavaduotojas',
    u'Frakcijos seniūnė',
    u'Frakcijos seniūnas',
]

def get_start_and_end_date(membership_date_list):
    return [
        None if s is None else datetime.strptime(s, '%Y-%m-%d').date()
        for s in membership_date_list
    ]

for row in data['rows']:
    value = row['value']
    name_parts = [value['first_name'], value['last_name']]
    full_name = u'{0} {1}'.format(*name_parts)
    sort_name = u'{1}, {0}'.format(*name_parts)
    parl_membership = get_membership_one_expected(value, 'parliament')
    parl_start_date, parl_end_date = get_start_and_end_date(
        parl_membership['membership']
    )
    term = get_term(parl_start_date, parl_end_date)
    if not term:
        continue
    # Some people seem to be missing a party field:
    try:
        party_membership = get_membership_one_expected(value, 'party')
    except NoGroupFound:
        party_membership = {}
    fraction_memberships = [
        g for g in get_memberships(value, 'fraction')
        if g['position'] in FRACTION_POSITIONS
    ]
    if not fraction_memberships:
        raise Exception(u'Found no fraction membership for {0}'.format(
            full_name)
        )
    for fraction_membership in fraction_memberships:
        fact_start_date, fact_end_date = get_start_and_end_date(
            fraction_membership['membership']
        )
        area, area_id = decompose_constituency(value)
        parl_start_date = max(parl_start_date, terms['11']['start_date'])
        clipped_start_date, clipped_end_date = clip_dates(
            fact_start_date, fact_end_date, parl_start_date, parl_end_date,
        )
        start_date_for_json = str(clipped_start_date) if clipped_start_date else None
        end_date_for_json = str(clipped_end_date) if clipped_end_date else None
        person_data = (
            {
                'id': value['source']['id'],
                'name': full_name,
                'website': value.get('home_page'),
                'image': value['photo'],
                'area': area,
                'area_id': area_id,
                'party': party_membership.get('name'),
                'faction': fraction_membership['name'],
                'birth_date': value.get('dob'),
                'source': value['source']['url'],
                'start_date': start_date_for_json,
                'end_date': end_date_for_json,
                'term': term,
                'given_name': value['first_name'],
                'family_name': value['last_name'],
                'email': get_email(value),
                'phone': get_phone(value),
                'sort_name': sort_name,
            }
        )
        scraperwiki.sqlite.save(
            unique_keys=['id', 'term', 'start_date'],
            data=person_data
        )
