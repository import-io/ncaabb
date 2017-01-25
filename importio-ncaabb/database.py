""" Import.io NCAABB Database models """
from flask_sqlalchemy import SQLAlchemy

db = SQLAlchemy()


class School(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(40), unique=True, nullable=False)
    conference = db.Column(db.String(40), nullable=False)
    teams = db.relationship('Team', backref=db.backref('school', lazy='joined'))
    school_snapshots = db.relationship('SchoolSnapshot', backref=db.backref('school', lazy='joined'))

    def __init__(self, name, conference):
        self.name = name
        self.conference = conference

    def __repr__(self):
        return 'School: %r' % self.name


class Team(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    school_id = db.Column(db.Integer, db.ForeignKey('school.id'), nullable=False)
    team_type = db.Column(db.String(5), nullable=False)
    team_snapshots = db.relationship('TeamSnapshot', backref=db.backref('team', lazy='joined'))

    def __init__(self, school, team_type):
        self.school = school
        self.team_type = team_type

    def __repr__(self):
        return ' %r Team: %r' % (self.school, self.team_type)


class SchoolSnapshot(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    school_id = db.Column(db.Integer, db.ForeignKey('school.id'), nullable=False)
    date = db.Column(db.DateTime, nullable=False)
    rank = db.Column(db.Integer, nullable=False)

    def __init__(self, school, date, rank):
        self.school = school
        self.date = date
        self.rank = rank

    def __repr__(self):
        return '%r: Rank = %r' % (self.school_id, self.rank)


class TeamSnapshot(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    team_id = db.Column(db.Integer, db.ForeignKey('team.id'), nullable=False)
    date = db.Column(db.DateTime, nullable=False)
    rank = db.Column(db.Integer, nullable=False)
    wins = db.Column(db.Integer, nullable=False)
    losses = db.Column(db.Integer, nullable=False)
    off_rank = db.Column(db.Integer)
    def_rank = db.Column(db.Integer)
    ppg = db.Column(db.Float)
    oppg = db.Column(db.Float)

    def __init__(self, team, date, rank, wins, losses, off_rank, def_rank, ppg, oppg):
        self.team = team
        self.date = date
        self.rank = rank
        self.wins = wins
        self.losses = losses
        self.off_rank = off_rank
        self.def_rank = def_rank
        self.ppg = ppg
        self.oppg = oppg

    def __repr__(self):
        return '%r: Off = %r, %r PPG. Def = %r, %r OPPG.' % (self.team_id, self.off_rank, self.ppg,
                                                             self.def_rank, self.oppg)
