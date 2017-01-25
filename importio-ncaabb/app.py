""" Import.io NCAABB Flask Application """
import os
from flask import Flask
from flask import render_template
from yaml import load
from apscheduler.schedulers.background import BackgroundScheduler
from fetch_data import *
from database import *

with open('config.yaml', 'r') as config_file:
    cfg = load(config_file)


def env_check():
    """ Function to check for and prompt input for missing environment variables """
    if not 'IMPORT_IO_API_KEY' in os.environ:
        os.environ['IMPORT_IO_API_KEY'] = \
            input('Please enter in your Import.io API Key: ')
    if not 'DATABASE_URL' in os.environ:
        os.environ['DATABASE_URL'] = \
            input('Please enter in your Database URL: ')

env_check()


def create_app():
    flask_app = Flask(__name__)
    flask_app.config['SQLALCHEMY_DATABASE_URI'] = os.environ['DATABASE_URL']
    flask_app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = cfg['SQLALCHEMY_TRACK_MODIFICATIONS']
    flask_app.config['RANKINGS_PER_PAGE'] = cfg['RANKINGS_PER_PAGE']
    db.init_app(flask_app)
    scheduler = BackgroundScheduler()
    scheduler.add_job(update_data, 'cron', hour=cfg['Job']['Hour'], minute=cfg['Job']['Minute'], args=[flask_app])
    scheduler.start()
    return flask_app

app = create_app()



@app.route('/')
def index():
    latest_date = SchoolSnapshot.query.order_by(SchoolSnapshot.date).first()
    top_ten_data = SchoolSnapshot.query.filter_by(date=latest_date.date).order_by(SchoolSnapshot.rank.asc()).join(
        School).limit(10)
    return render_template('index.html', schools=top_ten_data)


@app.route('/rankings/', defaults={'page': 1})
@app.route('/rankings/<int:page>')
def rankings(page):
    latest_date = SchoolSnapshot.query.order_by(SchoolSnapshot.date.desc()).first()
    rankings_data = SchoolSnapshot.query.filter_by(date=latest_date.date).order_by(SchoolSnapshot.rank.asc()).join(
        School).paginate(page, app.config['RANKINGS_PER_PAGE'])
    return render_template('rankings.html', schools=rankings_data)


@app.route('/school/<school_name>')
def school(school_name):
    view_school = School.query.filter_by(name=school_name).first()
    school_data = SchoolSnapshot.query.filter_by(school=view_school).all()
    team_data = Team.query.filter_by(school=view_school).join(TeamSnapshot).all()
    return render_template('school.html', school=school_data, teams=team_data)


@app.before_first_request
def init_db():
    db.create_all()
    if db.session.query(School).count() == 0:
        load_schools()
    if db.session.query(TeamSnapshot).count() == 0:
        get_snapshots()


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)
