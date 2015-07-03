"""SQLite module used to assit FAERS parse.py module

These functions are separated from parse.py to keep the DB modular. Feel free
to write your own for your own db.
"""


import sqlite3
from os.path import isfile
from os import remove
import zipfile
import zlib # Needed for zipfile compression type (not all systems may have this)

# Remove old database if exists and setup connection
# Maybe this should be an option passed to setupDB()
if isfile('faers-data.sqlite'):
    remove('faers-data.sqlite')
con = sqlite3.connect('faers-data.sqlite')


def setupDB():
    """
    setupDB() createds SQLite tables using the global connection.
    """
    con.execute("""
    create table demo (ISR integer, PRIMARYID integer, CASEID integer, CASEVERSION integer, CASE_NUM integer, I_F_COD text,
                    FOLL_SEQ text, IMAGE text, EVENT_DT date, MFR_DT date, INIT_FDA_DT date, FDA_DT date,
                    REPT_COD text, AUTH_NUM integer, MFR_NUM text, MFR_SNDR text, LIT_REF text, AGE numeric, AGE_COD text, AGE_GRP text, SEX text,
                    GNDR_COD text, E_SUB text, WT numeric, WT_COD text, REPT_DT date,
                    OCCP_COD text, DEATH_DT date, TO_MFR text, CONFID text, REPORTER_COUNTRY text, OCCR_COUNTRY text)
    """)
    con.execute("""
    create table drug (ISR integer, PRIMARYID integer, CASEID integer, DRUG_SEQ integer, ROLE_COD text,
                    DRUGNAME text, PROD_AI text, VAL_VBM integer, ROUTE text, DOSE_VBM text, CUM_DOSE_CHR real, CUM_DOSE_UNIT text, DECHAL text,
                    RECHAL text, LOT_NUM text, EXP_DT date, NDA_NUM text, DOSE_AMT real, DOSE_UNIT text, DOSE_FORM text, DOSE_FREQ text)
    """)
    con.execute("""
    create table react (ISR integer, PRIMARYID integer, CASEID integer, PT text not null, DRUG_REC_ACT text)
    """)
    con.execute("""
    create table outcome (ISR integer, PRIMARYID integer, CASEID integer, OUTC_COD text not null)
    """)
    con.execute("""
    create table source (ISR integer, PRIMARYID integer, CASEID integer, RPSR_COD text not null)
    """)
    con.execute("""
    create table therapy (ISR integer, PRIMARYID integer, CASEID integer, DRUG_SEQ integer, START_DT date,
                    END_DT date, DUR integer, DUR_COD text)
    """)
    con.execute("""
    create table indication (ISR integer, PRIMARYID integer, CASEID integer, DRUG_SEQ integer, INDI_DRUG_SEQ integer, INDI_PT text)
    """)
    con.commit()


def writeEntry(table_name, field_names, fields):
    """
    writeEntry() takes a table_name and list of fields and inserts them.
    """
    fs = ' (' + ','.join(field_names) + ')'
    qs = ['?'] * len(fields)
    stm = 'INSERT INTO ' + table_name + fs + ' VALUES(' + ', '.join(qs) + ')'
    con.execute(stm, fields)


def preClose():
    con.execute('VACUUM')


def closeDB():
    """
    closeDB() commits and closes the Db connection.
    """
    con.commit()
    con.close()


def postClose():
    if isfile('faers-data-sqlite.zip'):
        remove('faers-data-sqlite.zip')
    zf = zipfile.ZipFile('faers-data-sqlite.zip', 'w', allowZip64=True)
    zf.write('faers-data.sqlite', compress_type=zipfile.ZIP_DEFLATED)
    zf.close()
