""" Module for fetching data and cleaning from Import.io Extractors """
import math
import pandas as pd
import datetime
from yaml import load
from sqlalchemy import exc
from extractor import Extractor
from database import *

with open('config.yaml', 'r') as config_file:
    cfg = load(config_file)


def parse_resp(resp):
    """ Function for parsing variables from Import.io JSON response """
    output = []
    for page in resp:
        for group in page['result']['extractorData']['data']:
            for row in group['group']:
                output_row = dict()
                for variable in row.items():
                    output_row[variable[0]] = variable[1][0]['text']
                    try:
                        output_row['{0}_link'.format(variable[0])] = variable[1][0]['src']
                    except KeyError:
                        pass
                output.append(output_row)
    return output


def get_data(extractor_id):
    """ Function for fetching the latest JSON run from an Import.io Extractor """
    page_extractor = Extractor(extractor_id)
    resp = page_extractor.get_json()
    data = parse_resp(resp)
    data_df = pd.DataFrame.from_records(data)
    return data_df


def load_schools():
    """ Function for fetching schools from Import.io """
    men_rpi_df = get_data(cfg['Men']['RPI'])
    women_rpi_df = get_data(cfg['Women']['RPI'])
    schools_df = men_rpi_df.merge(women_rpi_df, how='outer', on='Team', suffixes=('_M', '_W'))
    schools_df = schools_df.sort_values('Team', ascending=True)
    for row in schools_df.iterrows():
        if pd.isnull(row[1]['Conference_W']):
            school = School(name=row[1]['Team'], conference=row[1]['Conference_M'])
            school.teams = [Team(school=school, team_type='Men')]
        elif pd.isnull(row[1]['Conference_M']):
            school = School(name=row[1]['Team'], conference=row[1]['Conference_W'])
            school.teams = [Team(school=school, team_type='Women')]
        else:
            school = School(name=row[1]['Team'], conference=row[1]['Conference_M'])
            school.teams = [Team(school=school, team_type='Men'),
                            Team(school=school, team_type='Women')]
        try:
            db.session.add(school)
            db.session.commit()
            print('{0} has been added to the database.'.format(school.name))
        except exc.IntegrityError:
            db.session().rollback()
            print('{0} is already in the database.'.format(school.name))
    print('Import.io NCAABB database has been created.')


def clean_data(team_type, category, stat, sort):
    """ Function for ranking offense and defense of teams """
    df = get_data(cfg[team_type][category])
    df = df.sort_values(stat, ascending=sort)
    df = df.reset_index(drop=True)
    df['{0} Rank'.format(category)] = (df.index + 1)
    return df


def get_team_snapshots(team_type):
    """ Function for fetching Team Snapshots from latest Import.io run """
    teams_df = get_data(cfg[team_type]['RPI'])
    offense_df = clean_data(team_type, 'Offense', 'PPG', False)
    defense_df = clean_data(team_type, 'Defense', 'OPPG', True)
    teams_df = teams_df.merge(offense_df, how='left', on='Team')
    teams_df = teams_df.merge(defense_df, how='left', on='Team')
    for row in teams_df.iterrows():
        school = School.query.filter_by(name=row[1]['Team']).first()
        team = Team.query.filter_by(school=school, team_type=team_type).first()
        rank = row[1]['RPI Rank']
        wins = row[1]['Wins']
        losses = row[1]['Losses']
        if math.isnan(row[1]['Offense Rank']):
            off_rank = None
        else:
            off_rank = row[1]['Offense Rank']
        if math.isnan(row[1]['Defense Rank']):
            def_rank = None
        else:
            def_rank = row[1]['Defense Rank']
        if math.isnan(float(row[1]['PPG'])):
            ppg = None
        else:
            ppg = row[1]['PPG']
        if math.isnan(float(row[1]['OPPG'])):
            oppg = None
        else:
            oppg = row[1]['OPPG']

        team_snapshot = TeamSnapshot(team=team, date=datetime.date.today(), rank=rank,
                                     wins=wins, losses=losses, off_rank=off_rank,
                                     def_rank=def_rank, ppg=ppg, oppg=oppg)
        db.session.add(team_snapshot)
        db.session.commit()
    print('{0}\'s Teams Snapshots have been saved.'.format(team_type))
    return teams_df


def get_school_snapshots(men_df, women_df):
    """ Function for fetching School Snapshots from latest Import.io run """
    schools_df = men_df.merge(women_df, how='inner', on='Team', suffixes=('_M', '_W'))
    rank_men = pd.Series(schools_df['RPI Rank_M']).astype(int)
    rank_women = pd.Series(schools_df['RPI Rank_W']).astype(int)
    schools_df['Score'] = (rank_men + rank_women) / 2
    schools_df = schools_df.sort_values('Score', ascending=True)
    schools_df = schools_df.reset_index(drop=True)
    schools_df['Overall Rank'] = (schools_df.index + 1)
    for row in schools_df.iterrows():
        school = School.query.filter_by(name=row[1]['Team']).first()
        school_snapshot = SchoolSnapshot(school=school, date=datetime.date.today(), rank=row[1]['Overall Rank'])
        db.session.add(school_snapshot)
    db.session.commit()
    print('School snapshots have been saved.')


def get_snapshots():
    """ Function for fetching new School and Team Snapshots from Import.io """
    print('Fetching new snapshots')
    men_df = get_team_snapshots('Men')
    women_df = get_team_snapshots('Women')
    get_school_snapshots(men_df, women_df)
    print('All snapshots have been saved.')


def update_data(app):
    """ Function for adding new snapshot in a background thread """
    with app.app_context():
        get_snapshots()
